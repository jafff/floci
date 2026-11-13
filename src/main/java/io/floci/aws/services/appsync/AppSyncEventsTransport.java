package io.floci.aws.services.appsync;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.floci.aws.core.common.AwsException;
import io.floci.aws.core.common.GlobalCorsFilter;
import io.floci.aws.core.common.RequestHost;
import io.floci.aws.core.common.Resettable;
import io.floci.aws.core.common.ServiceRegistry;
import io.floci.aws.core.common.WebSocketUpgrades;
import io.floci.aws.core.storage.AccountAwareStorageBackend;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** AWS Events wire protocol on the HTTP /event and WebSocket /event/realtime endpoints. */
@ApplicationScoped
public class AppSyncEventsTransport implements Resettable {
    private static final Logger LOG = Logger.getLogger(AppSyncEventsTransport.class);
    private static final String PROTOCOL = "aws-appsync-event-ws";
    private final AppSyncEventsService service;
    private final ObjectMapper mapper;
    private final Vertx vertx;
    private final ServiceRegistry registry;
    private final GlobalCorsFilter cors;
    private final Map<ServerWebSocket, Connection> connections = new ConcurrentHashMap<>();

    private record Connection(String account, String apiId, Map<String, String> subscriptions) {}

    @Inject
    public AppSyncEventsTransport(AppSyncEventsService service, ObjectMapper mapper, Vertx vertx, ServiceRegistry registry,
                                  GlobalCorsFilter cors) {
        this.service = service;
        this.mapper = mapper;
        this.vertx = vertx;
        this.registry = registry;
        this.cors = cors;
    }

    void routes(@Observes Router router) {
        router.post("/event").handler(this::http);
        router.get("/event/realtime").handler(this::upgrade);
    }

    private void enabled() {
        if (!registry.isServiceEnabled("appsync")) {
            throw new AwsException("ServiceUnavailableException", "AppSync is disabled", 503);
        }
    }

    private void http(RoutingContext context) {
        try {
            String host = RequestHost.of(context.request());
            if (service.apiIdForHost(host) == null) {
                context.next();
                return;
            }
            cors.applyResponseHeaders(context);
            enabled();
            AccountAwareStorageBackend.OwnedEntry<ObjectNode> api = service.resolve(host);
            String id = api.value().path("apiId").asText();
            ObjectNode authorization = mapper.createObjectNode();
            authorization.put("x-api-key", context.request().getHeader("x-api-key"));
            Buffer buffered = context.body().buffer();
            if (buffered != null) {
                publishHttp(context, api.account(), id, authorization, buffered);
            } else {
                context.request().body().onSuccess(body -> publishHttp(context, api.account(), id, authorization, body))
                        .onFailure(error -> httpError(context, AppSyncEventsService.bad("Could not read request body")));
                // Quarkus pauses requests before dispatching custom Vert.x routes.
                context.request().resume();
            }
        } catch (AwsException e) {
            httpError(context, e);
        }
    }

    private void publishHttp(RoutingContext context, String account, String id, ObjectNode authorization, Buffer body) {
        try {
            JsonNode request = parse(body.toString());
            String channel = service.channel(request, false);
            service.authorize(account, id, channel, "publish", authorization);
            ArrayNode events = service.events(request);
            ObjectNode result = publish(account, id, channel, events);
            context.response().putHeader("Content-Type", "application/json").end(result.toString());
        } catch (AwsException e) {
            httpError(context, e);
        }
    }

    private void upgrade(RoutingContext context) {
        String host = RequestHost.of(context.request());
        if (service.apiIdForHost(host) == null || !WebSocketUpgrades.isHandshake(context.request())) {
            context.next();
            return;
        }
        try {
            enabled();
            AccountAwareStorageBackend.OwnedEntry<ObjectNode> api = service.resolve(host);
            String id = api.value().path("apiId").asText();
            String offered = context.request().getHeader("Sec-WebSocket-Protocol");
            List<String> protocols = offered == null ? List.of() : Arrays.stream(offered.split(",")).map(String::trim).toList();
            if (!protocols.contains(PROTOCOL)) {
                throw AppSyncEventsService.bad("The aws-appsync-event-ws subprotocol is required");
            }
            String header = protocols.stream().filter(p -> p.startsWith("header-")).findFirst()
                    .orElseThrow(() -> AppSyncEventsService.bad("Authorization subprotocol is required"));
            JsonNode authorization;
            try {
                authorization = parse(new String(Base64.getUrlDecoder().decode(header.substring(7)), StandardCharsets.UTF_8));
            } catch (IllegalArgumentException e) {
                throw AppSyncEventsService.bad("Invalid authorization subprotocol");
            }
            if (!service.resolve(authorization.path("host").asText()).value().path("apiId").asText().equals(id)) {
                throw new AwsException("UnauthorizedException", "Authorization host does not match API", 401);
            }
            service.authorize(api.account(), id, null, "connect", authorization);
            context.request().toWebSocket().onSuccess(socket -> {
                Connection connection = new Connection(api.account(), id, new ConcurrentHashMap<>());
                connections.put(socket, connection);
                long timer = vertx.setPeriodic(60000, ignored -> send(socket, mapper.createObjectNode().put("type", "ka")));
                socket.textMessageHandler(message -> message(socket, connection, message));
                socket.closeHandler(ignored -> {
                    connections.remove(socket);
                    vertx.cancelTimer(timer);
                });
                socket.exceptionHandler(error -> {
                    LOG.debugv(error, "AppSync Events WebSocket failed for API {0}", id);
                    connections.remove(socket);
                    vertx.cancelTimer(timer);
                    socket.close();
                });
            }).onFailure(error -> LOG.debugv(error, "AppSync Events WebSocket upgrade failed"));
        } catch (AwsException e) {
            httpError(context, e);
        }
    }

    private void message(ServerWebSocket socket, Connection connection, String message) {
        String id = "";
        String type = "";
        try {
            JsonNode request = parse(message);
            type = request.path("type").asText();
            id = request.path("id").asText();
            if ("connection_init".equals(type)) {
                send(socket, mapper.createObjectNode().put("type", "connection_ack").put("connectionTimeoutMs", 300000));
                return;
            }
            if (!id.matches("[A-Za-z0-9_+-]{1,128}")) {
                throw AppSyncEventsService.bad("Invalid operation id");
            }
            ObjectNode result = switch (type) {
                case "subscribe" -> {
                    String channel = service.channel(request, true);
                    service.authorize(connection.account(), connection.apiId(), channel, "subscribe", request.path("authorization"));
                    if (connection.subscriptions().putIfAbsent(id, channel) != null) {
                        throw new AwsException("SubscriptionProcessingError", "Duplicate subscription id", 400);
                    }
                    yield mapper.createObjectNode();
                }
                case "unsubscribe" -> {
                    if (connection.subscriptions().remove(id) == null) {
                        throw new AwsException("UnknownOperationError", "Unknown operation id " + id, 400);
                    }
                    yield mapper.createObjectNode();
                }
                case "publish" -> {
                    String channel = service.channel(request, false);
                    service.authorize(connection.account(), connection.apiId(), channel, "publish", request.path("authorization"));
                    yield publish(connection.account(), connection.apiId(), channel, service.events(request));
                }
                default -> throw new AwsException("UnknownOperationError", "Unknown message type", 400);
            };
            send(socket, result.put("type", type + "_success").put("id", id));
        } catch (AwsException e) {
            ObjectNode error = mapper.createObjectNode().put("type",
                    List.of("subscribe", "unsubscribe", "publish").contains(type) ? type + "_error" : "error");
            if (!id.isEmpty()) {
                error.put("id", id);
            }
            error.putArray("errors").addObject().put("errorType", e.getErrorCode()).put("message", e.getMessage());
            send(socket, error);
        }
    }

    private ObjectNode publish(String account, String apiId, String channel, ArrayNode events) {
        ObjectNode result = mapper.createObjectNode();
        ArrayNode successful = result.putArray("successful");
        for (int i = 0; i < events.size(); i++) {
            successful.addObject().put("identifier", UUID.randomUUID().toString()).put("index", i);
        }
        result.putArray("failed");
        connections.forEach((socket, connection) -> {
            if (connection.account().equals(account) && connection.apiId().equals(apiId)) {
                connection.subscriptions().forEach((id, subscribed) -> {
                    if (matches(subscribed, channel)) {
                        ObjectNode data = mapper.createObjectNode().put("type", "data").put("id", id);
                        data.set("event", events);
                        send(socket, data);
                    }
                });
            }
        });
        return result;
    }

    static boolean matches(String subscription, String channel) {
        return subscription.equals(channel) || (subscription.endsWith("/*")
                && channel.startsWith(subscription.substring(0, subscription.length() - 1)));
    }

    public void invalidate(String apiId, String namespace) {
        connections.forEach((socket, connection) -> {
            if (connection.apiId().equals(apiId)) {
                if (namespace == null) {
                    socket.close();
                    connections.remove(socket);
                } else {
                    connection.subscriptions().entrySet().removeIf(entry ->
                            entry.getValue().equals(namespace) || entry.getValue().startsWith(namespace + "/"));
                }
            }
        });
    }

    private JsonNode parse(String text) {
        try {
            JsonNode node = mapper.readTree(text);
            if (node == null || !node.isObject()) {
                throw AppSyncEventsService.bad("Expected a JSON object");
            }
            return node;
        } catch (JsonProcessingException e) {
            throw AppSyncEventsService.bad("Invalid JSON");
        }
    }

    private void send(ServerWebSocket socket, ObjectNode message) {
        if (!socket.isClosed()) {
            socket.writeTextMessage(message.toString()).onFailure(error -> {
                LOG.debugv(error, "Could not send AppSync Events message");
                socket.close();
            });
        }
    }

    private void httpError(RoutingContext context, AwsException error) {
        context.request().resume();
        ObjectNode body = mapper.createObjectNode().put("message", error.getMessage());
        context.response().setStatusCode(error.getHttpStatus()).putHeader("Content-Type", "application/json")
                .putHeader("x-amzn-errortype", error.getErrorCode()).end(body.toString());
    }

    @Override
    @PreDestroy
    public void clear() {
        connections.keySet().forEach(ServerWebSocket::close);
        connections.clear();
    }
}
