package org.janelia.jacs2.rest.async.v2;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.janelia.jacs2.asyncservice.ServiceRegistry;
import org.janelia.model.service.ServiceMetaData;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.util.List;

@Api(value = "JACS Service Info")
@RequestScoped
@Produces("application/json")
@Path("/services")
public class ServiceMetadataResource {

    @Inject private ServiceRegistry serviceRegistry;

    @ApiOperation(value = "Get metadata about all services", notes = "")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    @GET
    @Path("/metadata")
    public Response getAllServicesMetadata() {
        List<ServiceMetaData> services = serviceRegistry.getAllServicesMetadata();
        return Response
                .status(Response.Status.OK)
                .entity(services)
                .build();
    }

    @ApiOperation(value = "Get metadata about a given service", notes = "")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 404, message = "If the service name is invalid"),
            @ApiResponse(code = 500, message = "Error occurred") })
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
