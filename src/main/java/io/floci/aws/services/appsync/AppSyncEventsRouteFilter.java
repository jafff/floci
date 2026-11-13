package io.floci.aws.services.appsync;

import io.floci.aws.core.common.ResolvedServiceCatalog;
import io.floci.aws.core.common.SignedServiceRoute;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.ext.Provider;

/** Disambiguates Event APIs from API Gateway's /v2/apis management routes. */
@Provider
@PreMatching
public class AppSyncEventsRouteFilter implements ContainerRequestFilter {
    private final SignedServiceRoute route;

    @Inject
    public AppSyncEventsRouteFilter(ResolvedServiceCatalog catalog, AppSyncEventsService service) {
        route = new SignedServiceRoute(catalog, "appsync", "/_appsync-events", path -> {
            String[] parts = path.split("/");
            if (path.equals("/v2/apis") || path.matches("/v2/apis/[^/]+")) {
                return true;
            }
            return parts.length >= 5 && "apis".equals(parts[2]) && service.contains(parts[3])
                    && (("v2".equals(parts[1]) && "channelNamespaces".equals(parts[4]))
                    || ("v1".equals(parts[1]) && "apikeys".equals(parts[4])));
        });
    }

    @Override
    public void filter(ContainerRequestContext context) {
        route.apply(context);
    }
}
