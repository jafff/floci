package io.floci.aws.services.appsync;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.floci.aws.config.EmulatorConfig;
import io.floci.aws.core.common.AwsException;
import io.floci.aws.core.common.RegionResolver;
import io.floci.aws.testing.InMemoryStorageFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AppSyncEventsServiceTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private AppSyncEventsService service;
    private String id;

    static final String CONFIG = """
            {"name":"events","eventConfig":{
              "authProviders":[{"authType":"API_KEY"}],
              "connectionAuthModes":[{"authType":"API_KEY"}],
              "defaultPublishAuthModes":[{"authType":"API_KEY"}],
              "defaultSubscribeAuthModes":[{"authType":"API_KEY"}]}}
            """;

    @BeforeEach
    void setup() throws Exception {
        EmulatorConfig config = mock(EmulatorConfig.class);
        when(config.effectiveBaseUrl()).thenReturn("http://localhost:4566");
        service = new AppSyncEventsService(new InMemoryStorageFactory("000000000000"), mapper,
                new RegionResolver("us-east-1", "000000000000"), config,
                Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC));
        id = service.createApi(mapper.readTree(CONFIG), "us-east-1").path("apiId").asText();
        service.createNamespace(id, mapper.createObjectNode().put("name", "chat"));
    }

    @Test
    void keysAreScopedToApiAndDeletionCascades() throws Exception {
        String key = service.createKey(id, mapper.createObjectNode()).path("id").asText();
        ObjectNode auth = mapper.createObjectNode().put("x-api-key", key);
        service.authorize("000000000000", id, "chat/room", "publish", auth);
        String other = service.createApi(mapper.readTree(CONFIG), "us-east-1").path("apiId").asText();
        assertThrows(AwsException.class, () -> service.authorize("000000000000", other, null, "connect", auth));
        service.deleteKey(id, key);
        assertThrows(AwsException.class, () -> service.authorize("000000000000", id, null, "connect", auth));
        service.deleteApi(id);
        assertThrows(AwsException.class, () -> service.getNamespace(id, "chat"));
        assertThrows(AwsException.class, () -> service.resolve(id + ".localhost:4566"));
    }

    @Test
    void invalidUpdateLeavesExistingStateUntouched() {
        assertThrows(AwsException.class, () -> service.updateApi(id, mapper.createObjectNode().put("name", "")));
        assertEquals("events", service.getApi(id).path("name").asText());
        assertThrows(AwsException.class, () -> service.updateNamespace(id, "chat",
                mapper.createObjectNode().put("codeHandlers", "export function onPublish() {}")));
        assertFalse(service.getNamespace(id, "chat").has("codeHandlers"));
    }

    @Test
    void validatesChannelsEventsAndWildcardBoundaries() throws Exception {
        assertEquals("chat/room", service.channel(mapper.createObjectNode().put("channel", "/chat/room/"), false));
        assertThrows(AwsException.class, () -> service.channel(mapper.createObjectNode().put("channel", "chat/*"), false));
        assertThrows(AwsException.class, () -> service.channel(mapper.createObjectNode().put("channel", "chat//room"), true));
        assertTrue(AppSyncEventsTransport.matches("chat/*", "chat/room/subroom"));
        assertFalse(AppSyncEventsTransport.matches("chat/*", "chatty/room"));
        assertFalse(AppSyncEventsTransport.matches("chat/*", "chat"));
        assertEquals(2, service.events(mapper.readTree("{\"events\":[\"null\",\"{\\\"x\\\":1}\"]}")).size());
        for (String invalid : new String[]{"{\"events\":[]}", "{\"events\":[{}]}", "{\"events\":[\"{} {}\"]}"}) {
            assertThrows(AwsException.class, () -> service.events(mapper.readTree(invalid)));
        }
    }

    @Test
    void paginatesWithoutRepeatingItems() throws Exception {
        service.createApi(mapper.readTree(CONFIG), "us-east-1");
        ObjectNode first = service.listApis(1, null);
        ObjectNode second = service.listApis(1, first.path("nextToken").asText());
        assertNotEquals(first.path("apis").get(0).path("apiId"), second.path("apis").get(0).path("apiId"));
        assertFalse(second.has("nextToken"));
        assertThrows(AwsException.class, () -> service.listApis(0, null));
    }

    @Test
    void createsPartitionAwareArns() throws Exception {
        ObjectNode api = service.createApi(mapper.readTree(CONFIG), "cn-north-1");
        assertTrue(api.path("apiArn").asText().startsWith("arn:aws-cn:appsync:cn-north-1:"));
    }
}
