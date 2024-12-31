package org.janelia.jacs2.rest.async.v2;

import java.util.List;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.janelia.jacs2.asyncservice.ServiceRegistry;
import org.janelia.model.service.ServiceMetaData;

@Tag(name = "ServiceMetadata", description = "JACS Service Info")
@RequestScoped
@Produces("application/json")
@Path("/services")
public class ServiceMetadataResource {

    @Inject private ServiceRegistry serviceRegistry;

    @Operation(summary = "Get metadata about all services")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Success"),
            @ApiResponse(responseCode = "500", description = "Error occurred") })
    @GET
    @Path("/metadata")
    public Response getAllServicesMetadata() {
        List<ServiceMetaData> services = serviceRegistry.getAllServicesMetadata();
        return Response
                .status(Response.Status.OK)
                .entity(services)
                .build();
    }

    @Operation(summary = "Get metadata about a given service")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Success"),
            @ApiResponse(responseCode = "404", description = "If the service name is invalid"),
            @ApiResponse(responseCode = "500", description = "Error occurred") })
    @GET
    @Path("/metadata/{service-name}")
    public Response getServiceMetadata(@PathParam("service-name") String serviceName) {
        ServiceMetaData smd = serviceRegistry.getServiceMetadata(serviceName);
        if (smd == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        }
        return Response
                .status(Response.Status.OK)
                .entity(smd)
                .build();
    }

}
