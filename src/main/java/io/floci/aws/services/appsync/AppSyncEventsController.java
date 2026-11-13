package io.floci.aws.services.appsync;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.floci.aws.core.common.RegionResolver;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path(AppSyncEventsRouteFilter.INTERNAL_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class AppSyncEventsController {
    private final AppSyncEventsService service;
    private final RegionResolver regions;
    private final ObjectMapper mapper;
    private final AppSyncEventsTransport transport;

    @Inject
    public AppSyncEventsController(AppSyncEventsService service, RegionResolver regions, ObjectMapper mapper,
                                   AppSyncEventsTransport transport) {
        this.service = service;
        this.regions = regions;
        this.mapper = mapper;
        this.transport = transport;
    }

    @POST
    @Path("/v2/apis")
    public ObjectNode createApi(@Context HttpHeaders headers, JsonNode request) {
        return wrap("api", service.createApi(request, regions.resolveRegion(headers)));
    }

    @GET
    @Path("/v2/apis/{apiId}")
    public ObjectNode getApi(@PathParam("apiId") String id) {
        return wrap("api", service.getApi(id));
    }

    @POST
    @Path("/v2/apis/{apiId}")
    public ObjectNode updateApi(@PathParam("apiId") String id, JsonNode request) {
        return wrap("api", service.updateApi(id, request));
    }

    @DELETE
    @Path("/v2/apis/{apiId}")
    public Response deleteApi(@PathParam("apiId") String id) {
        service.deleteApi(id);
        transport.invalidate(id, null);
        return Response.ok().build();
    }

    @GET
    @Path("/v2/apis")
    public ObjectNode listApis(@QueryParam("maxResults") Integer limit, @QueryParam("nextToken") String token) {
        return service.listApis(limit, token);
    }

    private ObjectNode wrap(String field, ObjectNode value) {
        return mapper.createObjectNode().set(field, value);
    }
}
