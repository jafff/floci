package io.floci.aws.services.appsync;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.floci.aws.config.EmulatorConfig;
import io.floci.aws.core.common.AwsArnUtils;
import io.floci.aws.core.common.AwsException;
import io.floci.aws.core.common.RegionResolver;
import io.floci.aws.core.storage.AccountAwareStorageBackend;
import io.floci.aws.core.storage.StorageFactory;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.net.URI;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

/** Management state for Event APIs. Live subscriptions are owned by the transport. */
@ApplicationScoped
public class AppSyncEventsService {
    private final AccountAwareStorageBackend<ObjectNode> apis;
    private final AccountAwareStorageBackend<ObjectNode> namespaces;
    private final AccountAwareStorageBackend<ObjectNode> keys;
    private final ObjectMapper mapper;
    private final RegionResolver regions;
    private final Clock clock;
    private final String authority;
    private final String baseHostname;

    @Inject
    public AppSyncEventsService(StorageFactory storage, ObjectMapper mapper, RegionResolver regions,
                               EmulatorConfig config, Clock clock) {
        apis = storage.create("appsync", "appsync-event-apis.json", new TypeReference<>() {});
        namespaces = storage.create("appsync", "appsync-event-namespaces.json", new TypeReference<>() {});
        keys = storage.create("appsync", "appsync-event-keys.json", new TypeReference<>() {});
        this.mapper = mapper;
        this.regions = regions;
        this.clock = clock;
        URI base = URI.create(config.effectiveBaseUrl());
        baseHostname = base.getHost();
        String host = base.getHost();
        if ("localhost".equals(host) || "127.0.0.1".equals(host) || "[::1]".equals(host)) {
            host = "localhost.floci.io";
        }
        authority = host + (base.getPort() < 0 ? "" : ":" + base.getPort());
    }

    public boolean contains(String id) {
        return apis.findAnyAccount(id).isPresent();
    }

    public synchronized ObjectNode createApi(JsonNode request, String region) {
        validateApi(request);
        String id = UUID.randomUUID().toString().replace("-", "").substring(0, 26);
        ObjectNode api = mapper.createObjectNode();
        copy(request, api, "name", "eventConfig", "ownerContact", "tags");
        api.put("apiId", id);
        api.put("apiArn", regions.buildArn("appsync", region, "apis/" + id));
        api.put("created", clock.instant().getEpochSecond());
        api.put("xrayEnabled", false);
        api.putObject("dns").put("HTTP", id + "." + authority).put("REALTIME", id + "." + authority);
        apis.put(id, api);
        return api.deepCopy();
    }

    public ObjectNode getApi(String id) {
        return apis.get(id).filter(this::inRegion).orElseThrow(() -> missing("API")).deepCopy();
    }

    public synchronized ObjectNode updateApi(String id, JsonNode request) {
        ObjectNode api = getApi(id);
        copy(request, api, "name", "eventConfig", "ownerContact");
        validateApi(api);
        apis.put(id, api);
        return api.deepCopy();
    }

    public synchronized void deleteApi(String id) {
        getApi(id);
        namespaces.keys().stream().filter(k -> k.startsWith(id + "/")).forEach(namespaces::delete);
        keys.keys().stream().filter(k -> k.startsWith(id + "/")).forEach(keys::delete);
        apis.delete(id);
    }

    public ObjectNode listApis(Integer limit, String token) {
        return page("apis", new ArrayList<>(apis.scan(k -> true).stream().filter(this::inRegion).toList()),
                "apiId", limit, token);
    }

    private boolean inRegion(ObjectNode api) {
        return AwsArnUtils.parse(api.path("apiArn").asText()).region().equals(regions.getRegion());
    }

    public synchronized ObjectNode createNamespace(String id, JsonNode request) {
        ObjectNode api = getApi(id);
        String name = request.path("name").asText();
        if (!name.matches("[A-Za-z0-9](?:[A-Za-z0-9-]{0,48}[A-Za-z0-9])?")) {
            throw bad("Invalid namespace name");
        }
        if (namespaces.get(id + "/" + name).isPresent()) {
            throw new AwsException("ConflictException", "Namespace already exists", 409);
        }
        ObjectNode ns = mapper.createObjectNode();
        copy(request, ns, "name", "publishAuthModes", "subscribeAuthModes", "tags", "codeHandlers", "handlerConfigs");
        validateNamespace(ns);
        ns.put("apiId", id);
        ns.put("channelNamespaceArn", api.path("apiArn").asText() + "/channelNamespace/" + name);
        ns.put("created", clock.instant().getEpochSecond());
        ns.put("lastModified", clock.instant().getEpochSecond());
        namespaces.put(id + "/" + name, ns);
        return ns.deepCopy();
    }

    public ObjectNode getNamespace(String id, String name) {
        getApi(id);
        return namespaces.get(id + "/" + name).orElseThrow(() -> missing("Namespace")).deepCopy();
    }

    public synchronized ObjectNode updateNamespace(String id, String name, JsonNode request) {
        ObjectNode ns = getNamespace(id, name);
        copy(request, ns, "publishAuthModes", "subscribeAuthModes", "codeHandlers", "handlerConfigs");
        validateNamespace(ns);
        ns.put("lastModified", clock.instant().getEpochSecond());
        namespaces.put(id + "/" + name, ns);
        return ns.deepCopy();
    }

    public synchronized void deleteNamespace(String id, String name) {
        getNamespace(id, name);
        namespaces.delete(id + "/" + name);
    }

    public ObjectNode listNamespaces(String id, Integer limit, String token) {
        getApi(id);
        return page("channelNamespaces", namespaces.scan(k -> k.startsWith(id + "/")), "name", limit, token);
    }

    public synchronized ObjectNode createKey(String id, JsonNode request) {
        getApi(id);
        if (keys.scan(k -> k.startsWith(id + "/")).size() >= 2) {
            throw new AwsException("ApiKeyLimitExceededException", "The API key exceeded a limit.", 400);
        }
        ObjectNode key = mapper.createObjectNode();
        key.put("id", "da2-" + UUID.randomUUID().toString().replace("-", "").substring(0, 26));
        applyKey(key, request);
        keys.put(id + "/" + key.path("id").asText(), key);
        return key.deepCopy();
    }

    public synchronized ObjectNode updateKey(String id, String keyId, JsonNode request) {
        getApi(id);
        ObjectNode key = keys.get(id + "/" + keyId).orElseThrow(() -> missing("API key")).deepCopy();
        applyKey(key, request);
        keys.put(id + "/" + keyId, key);
        return key.deepCopy();
    }

    public synchronized void deleteKey(String id, String keyId) {
        getApi(id);
        keys.get(id + "/" + keyId).orElseThrow(() -> missing("API key"));
        keys.delete(id + "/" + keyId);
    }

    public ObjectNode listKeys(String id, Integer limit, String token) {
        getApi(id);
        return page("apiKeys", keys.scan(k -> k.startsWith(id + "/")), "id", limit, token);
    }

    private void applyKey(ObjectNode key, JsonNode request) {
        long now = clock.instant().getEpochSecond();
        long expires = request.has("expires") ? request.path("expires").asLong()
                : key.path("expires").asLong(now + 7 * 86400);
        if ((request.has("expires") || !key.has("expires"))
                && (expires < now + 86400 || expires > now + 365 * 86400L)) {
            throw new AwsException("ApiKeyValidityOutOfBoundsException", "Expiry must be 1 to 365 days from now", 400);
        }
        expires = expires / 3600 * 3600;
        key.put("expires", expires);
        key.put("deletes", expires + 60 * 86400L);
        copy(request, key, "description");
    }

    /** A public API hostname identifies its owner even without a signed management request. */
    public AccountAwareStorageBackend.OwnedEntry<ObjectNode> resolve(String host) {
        String id = apiIdForHost(host);
        if (id == null) {
            throw missing("API");
        }
        return apis.findAnyAccountEntry(id).orElseThrow(() -> missing("API"));
    }

    public String apiIdForHost(String host) {
        return AppSyncEventsHost.apiId(host, baseHostname);
    }

    public void authorize(String account, String id, String channel, String operation, JsonNode authorization) {
        ObjectNode api = apis.getForAccount(account, id).orElseThrow(() -> missing("API"));
        JsonNode config = api.path("eventConfig");
        JsonNode modes = config.path("connectionAuthModes");
        if (!"connect".equals(operation)) {
            ObjectNode ns = namespaces.getForAccount(account, id + "/" + channel.split("/")[0])
                    .orElseThrow(() -> missing("Namespace"));
            String field = operation + "AuthModes";
            modes = ns.hasNonNull(field) && !ns.path(field).isEmpty() ? ns.path(field)
                    : config.path("default" + Character.toUpperCase(operation.charAt(0)) + operation.substring(1) + "AuthModes");
        }
        boolean apiKeyAllowed = false;
        for (JsonNode mode : modes) {
            apiKeyAllowed |= "API_KEY".equals(mode.path("authType").asText());
        }
        String value = authorization.path("x-api-key").asText();
        ObjectNode key = keys.getForAccount(account, id + "/" + value).orElse(null);
        if (!apiKeyAllowed || key == null || key.path("expires").asLong() <= clock.instant().getEpochSecond()) {
            throw new AwsException("UnauthorizedException", "Not authorized", 401);
        }
    }

    public String channel(JsonNode request, boolean subscription) {
        String channel = request.path("channel").asText();
        if (channel.startsWith("/")) {
            channel = channel.substring(1);
        }
        if (channel.endsWith("/")) {
            channel = channel.substring(0, channel.length() - 1);
        }
        String[] segments = channel.split("/", -1);
        if (segments.length > 5) {
            throw bad("A channel can contain at most five segments");
        }
        for (int i = 0; i < segments.length; i++) {
            if (subscription && i > 0 && i == segments.length - 1 && "*".equals(segments[i])) {
                continue;
            }
            if (!segments[i].matches("[A-Za-z0-9](?:[A-Za-z0-9-]{0,48}[A-Za-z0-9])?")) {
                throw bad("Invalid channel");
            }
        }
        return channel;
    }

    public ArrayNode events(JsonNode request) {
        JsonNode events = request.path("events");
        if (!events.isArray() || events.isEmpty() || events.size() > 5) {
            throw bad("A batch must contain one to five events");
        }
        for (JsonNode event : events) {
            try {
                if (!event.isTextual() || event.asText().isBlank()) {
                    throw bad("Events must be stringified JSON values");
                }
                JsonNode parsed = mapper.reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readTree(event.asText());
                if (parsed == null || parsed.isMissingNode()) {
                    throw bad("Events must be stringified JSON values");
                }
            } catch (JsonProcessingException e) {
                throw bad("Events must be stringified JSON values");
            }
        }
        return (ArrayNode) events.deepCopy();
    }

    private void validateApi(JsonNode api) {
        if (!api.path("name").asText().matches("[A-Za-z0-9_ -]{1,50}")) {
            throw bad("Invalid API name");
        }
        JsonNode config = api.path("eventConfig");
        for (String field : List.of("authProviders", "connectionAuthModes", "defaultPublishAuthModes", "defaultSubscribeAuthModes")) {
            validateModes(config.path(field));
        }
    }

    private void validateNamespace(JsonNode ns) {
        for (String field : List.of("publishAuthModes", "subscribeAuthModes")) {
            if (ns.hasNonNull(field) && !ns.path(field).isEmpty()) {
                validateModes(ns.path(field));
            }
        }
        if (!ns.path("codeHandlers").asText("").isEmpty() || ns.hasNonNull("handlerConfigs")) {
            throw bad("Event handlers and data source integrations are not yet supported");
        }
    }

    private void validateModes(JsonNode modes) {
        if (!modes.isArray() || modes.isEmpty()) {
            throw bad("Authorization modes are required");
        }
        for (JsonNode mode : modes) {
            if (!"API_KEY".equals(mode.path("authType").asText())) {
                throw bad("Only API_KEY authorization is currently supported for Event APIs");
            }
        }
    }

    private ObjectNode page(String field, List<ObjectNode> items, String sortField, Integer limit, String token) {
        if (limit != null && (limit < 1 || limit > 25)) {
            throw bad("maxResults must be between 1 and 25");
        }
        items.sort(Comparator.comparing(item -> item.path(sortField).asText()));
        AppSyncService.Page<ObjectNode> page = AppSyncService.paginate(items, token, limit == null ? 25 : limit);
        ObjectNode response = mapper.createObjectNode();
        response.set(field, mapper.valueToTree(page.items()));
        if (page.nextToken() != null) {
            response.put("nextToken", page.nextToken());
        }
        return response;
    }

    private static void copy(JsonNode from, ObjectNode to, String... fields) {
        for (String field : fields) {
            if (from.has(field)) {
                to.set(field, from.get(field).deepCopy());
            }
        }
    }

    static AwsException bad(String message) {
        return new AwsException("BadRequestException", message, 400);
    }

    private static AwsException missing(String resource) {
        return new AwsException("NotFoundException", resource + " not found", 404);
    }
}
