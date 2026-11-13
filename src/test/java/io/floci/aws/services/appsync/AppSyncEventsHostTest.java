package io.floci.aws.services.appsync;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class AppSyncEventsHostTest {
    private static final String ID = "abcdefghijklmnopqrstuvwxyz";

    @Test
    void recognizesLocalAndConfiguredAuthorities() {
        assertEquals(ID, AppSyncEventsHost.apiId(ID + ".localhost:4566", "emulator.test"));
        assertEquals(ID, AppSyncEventsHost.apiId(ID.toUpperCase() + ".LOCALHOST.FLOCI.IO.:4566", "emulator.test"));
        assertEquals(ID, AppSyncEventsHost.apiId(ID + ".emulator.test:4566", "emulator.test"));
    }

    @Test
    void recognizesAwsHttpAndRealtimeEndpointsAcrossPartitions() {
        assertEquals(ID, AppSyncEventsHost.apiId(ID + ".appsync-api.us-east-1.amazonaws.com", "localhost"));
        assertEquals(ID, AppSyncEventsHost.apiId(ID + ".appsync-realtime-api.cn-north-1.amazonaws.com.cn", "localhost"));
    }

    @Test
    void rejectsUnrelatedAndMalformedHosts() {
        for (String host : new String[]{null, "", "localhost", "[::1]:4566", ID + ".example.com",
                ID + ".localhost.evil.test", ID + ".appsync-api.not-a-region.amazonaws.com",
                ID + ".appsync-api.us-east-1.example.com", "short.localhost", "bad host"}) {
            assertNull(AppSyncEventsHost.apiId(host, "localhost"), host);
        }
    }
}
