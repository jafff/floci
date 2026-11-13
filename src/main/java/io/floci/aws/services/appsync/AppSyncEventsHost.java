package io.floci.aws.services.appsync;

import io.floci.aws.core.common.AwsPartitions;
import io.floci.aws.core.common.RegionResolver;

import java.net.URI;
import java.util.Locale;

/** Host recognition shared by HTTP publish and WebSocket upgrade routing. */
final class AppSyncEventsHost {
    private AppSyncEventsHost() {
    }

    static String apiId(String authority, String baseHostname) {
        if (authority == null || authority.isBlank()) {
            return null;
        }
        String host;
        try {
            host = URI.create("http://" + authority.trim()).getHost();
        } catch (IllegalArgumentException ignored) {
            // An invalid authority cannot identify an AppSync endpoint.
            return null;
        }
        if (host == null) {
            return null;
        }
        host = host.toLowerCase(Locale.ROOT);
        if (host.endsWith(".")) {
            host = host.substring(0, host.length() - 1);
        }
        int dot = host.indexOf('.');
        if (dot != 26 || !host.substring(0, dot).matches("[a-z0-9]{26}")) {
            return null;
        }
        String suffix = host.substring(dot + 1);
        if (suffix.equals("localhost") || suffix.equals("localhost.floci.io")
                || suffix.equalsIgnoreCase(baseHostname)) {
            return host.substring(0, dot);
        }
        String[] labels = suffix.split("\\.", 3);
        if (labels.length == 3 && (labels[0].equals("appsync-api") || labels[0].equals("appsync-realtime-api"))
                && RegionResolver.isKnownRegion(labels[1]) && AwsPartitions.isDnsSuffix(labels[2])) {
            return host.substring(0, dot);
        }
        return null;
    }
}
