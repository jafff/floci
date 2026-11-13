package io.floci.aws.services.appsync;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.vertx.core.Vertx;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketClient;
import io.vertx.core.http.WebSocketConnectOptions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.appsync.AppSyncClient;
import software.amazon.awssdk.services.appsync.model.Api;
import software.amazon.awssdk.services.appsync.model.AuthenticationType;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestProfile(AppSyncEventsIntegrationTest.EventsProfile.class)
class AppSyncEventsIntegrationTest {
    public static class EventsProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("floci.security.extra-cors-allowed-origins", "http://localhost:3000");
        }
    }

    @TestHTTPResource("/")
    URI endpoint;

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void ordinaryHttpRequestsRemainAvailableToS3() {
        given().put("/event").then().statusCode(200);
        try {
            given().contentType("application/octet-stream").body("ordinary object".getBytes(StandardCharsets.UTF_8))
                    .put("/event/realtime").then().statusCode(200);
            assertEquals("ordinary object", given().get("/event/realtime").then().statusCode(200).extract().asString());
        } finally {
            given().delete("/event/realtime");
            given().delete("/event");
        }
    }

    @Test
    void apiGatewayRoutesRemainAvailable() {
        String authorization = "AWS4-HMAC-SHA256 Credential=test/20261113/us-east-1/apigateway/aws4_request";
        String id = given().header("Authorization", authorization).contentType("application/json")
                .body("{\"name\":\"events-routing-regression\",\"protocolType\":\"HTTP\"}")
                .post("/v2/apis").then().statusCode(201).extract().path("apiId");
        try {
            given().header("Authorization", authorization).get("/v2/apis/" + id)
                    .then().statusCode(200).body("protocolType", equalTo("HTTP"));
        } finally {
            given().header("Authorization", authorization).delete("/v2/apis/" + id).then().statusCode(204);
        }
    }

    @Test
    void sdkManagementAndHttpWebSocketPubSub() throws Exception {
        try (AppSyncClient client = AppSyncClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))).build()) {
            Api api = client.createApi(b -> b.name("events-integration").eventConfig(c -> c
                    .authProviders(p -> p.authType(AuthenticationType.API_KEY))
                    .connectionAuthModes(m -> m.authType(AuthenticationType.API_KEY))
                    .defaultPublishAuthModes(m -> m.authType(AuthenticationType.API_KEY))
                    .defaultSubscribeAuthModes(m -> m.authType(AuthenticationType.API_KEY)))).api();
            String id = api.apiId();
            Vertx vertx = Vertx.vertx();
            WebSocketClient sockets = vertx.createWebSocketClient();
            try {
                assertEquals("events-integration", client.getApi(b -> b.apiId(id)).api().name());
                assertNotNull(api.created());
                given().header("Authorization", "AWS4-HMAC-SHA256 Credential=111111111111/20261113/us-east-1/appsync/aws4_request")
                        .get("/v2/apis/" + id).then().statusCode(404);
                client.updateApi(b -> b.apiId(id).name("renamed"));
                assertEquals("renamed", client.getApi(b -> b.apiId(id)).api().name());
                client.createChannelNamespace(b -> b.apiId(id).name("chat"));
                assertNotNull(client.getChannelNamespace(b -> b.apiId(id).name("chat")).channelNamespace().created());
                assertEquals(1, client.listChannelNamespaces(b -> b.apiId(id)).channelNamespaces().size());
                String key = client.createApiKey(b -> b.apiId(id)).apiKey().id();
                String host = id + ".localhost:" + endpoint.getPort();
                String auth = mapper.createObjectNode().put("host", host).put("x-api-key", key).toString();
                String header = "header-" + Base64.getUrlEncoder().withoutPadding().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
                WebSocket socket = sockets.connect(new WebSocketConnectOptions().setHost("localhost").setPort(endpoint.getPort())
                        .setURI("/event/realtime").addHeader("Host", host)
                        .setSubProtocols(List.of("aws-appsync-event-ws", header)))
                        .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
                assertEquals("aws-appsync-event-ws", socket.subProtocol());
                LinkedBlockingQueue<String> messages = new LinkedBlockingQueue<>();
                socket.textMessageHandler(messages::add);
                socket.writeTextMessage("{\"type\":\"connection_init\"}");
                assertEquals(300000, receive(messages, "connection_ack").path("connectionTimeoutMs").asInt());
                String subscribe = "{\"type\":\"subscribe\",\"id\":\"sub\",\"channel\":\"/chat/*\",\"authorization\":" + auth + "}";
                socket.writeTextMessage(subscribe);
                receive(messages, "subscribe_success");
                socket.writeTextMessage(subscribe);
                receive(messages, "subscribe_error");
                given().header("Host", host).header("Origin", "http://localhost:3000")
                        .header("Access-Control-Request-Method", "POST").header("Access-Control-Request-Headers", "x-api-key")
                        .options("/event").then().statusCode(204).header("Access-Control-Allow-Origin", "http://localhost:3000");
                given().header("Host", host).header("x-api-key", key).header("Origin", "http://localhost:3000").contentType("application/json")
                        .body("{\"channel\":\"chat/room\",\"events\":[\"{\\\"hello\\\":1}\"]}")
                        .post("/event").then().statusCode(200).header("Access-Control-Allow-Origin", "http://localhost:3000")
                        .body("successful[0].index", equalTo(0)).body("failed", empty());
                assertEquals("{\"hello\":1}", receive(messages, "data").path("event").get(0).asText());
                socket.writeTextMessage("{\"type\":\"publish\",\"id\":\"pub\",\"channel\":\"chat/room\",\"events\":[\"42\"],\"authorization\":" + auth + "}");
                receive(messages, "data");
                receive(messages, "publish_success");
                socket.writeTextMessage("{\"type\":\"unsubscribe\",\"id\":\"sub\"}");
                receive(messages, "unsubscribe_success");
                given().header("Host", host).header("x-api-key", key).contentType("application/json")
                        .body("{\"channel\":\"chat/room\",\"events\":[\"42\"]}").post("/event").then().statusCode(200);
                assertNull(messages.poll(250, TimeUnit.MILLISECONDS), "Unsubscribed clients must not receive events");
                given().header("Host", host).header("x-api-key", key).contentType("application/json")
                        .body("{\"channel\":\"chat/room\",\"events\":[{}]}").post("/event").then().statusCode(400);
                given().header("Host", host).header("x-api-key", "invalid").contentType("application/json")
                        .body("{\"channel\":\"chat/room\",\"events\":[\"42\"]}").post("/event").then().statusCode(401);
                socket.close();
                client.deleteApiKey(b -> b.apiId(id).id(key));
                assertTrue(client.listApiKeys(b -> b.apiId(id)).apiKeys().isEmpty());
            } finally {
                sockets.close();
                vertx.close();
                client.deleteApi(b -> b.apiId(id));
            }
        }
    }

    private JsonNode receive(LinkedBlockingQueue<String> messages, String type) throws Exception {
        String message = messages.poll(10, TimeUnit.SECONDS);
        assertNotNull(message, "Timed out waiting for " + type);
        JsonNode node = mapper.readTree(message);
        assertEquals(type, node.path("type").asText(), message);
        return node;
    }
}
