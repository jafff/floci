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
    static final String INTERNAL_PREFIX = "/_appsync-events";
    private final SignedServiceRoute route;

    @Inject
    public AppSyncEventsRouteFilter(ResolvedServiceCatalog catalog) {
        route = new SignedServiceRoute(catalog, "appsync", INTERNAL_PREFIX,
                path -> path.equals("/v2/apis") || path.matches("/v2/apis/[^/]+"));
    }

    @Override
    public void filter(ContainerRequestContext context) {
        route.apply(context);
    }
}
