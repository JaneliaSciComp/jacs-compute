package org.janelia.jacs2.rest.async.v2;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.dataservice.cronservice.CronScheduledServiceManager;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.service.JacsScheduledServiceData;

@Tag(name = "ScheduledServiceInfo", description = "Scheduled JACS Services")
@RequestScoped
@Produces("application/json")
@Path("/scheduled-services")
public class ScheduledServiceInfoResource {
    private static final int DEFAULT_PAGE_SIZE = 100;

    @Inject
    private CronScheduledServiceManager jacsScheduledServiceDataManager;

    @Operation(summary = "Create a scheduled service entry")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Success"),
            @ApiResponse(responseCode = "500", description = "Error occurred")})
    @RequireAuthentication
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createScheduledService(JacsScheduledServiceData scheduledServiceData) {
        JacsScheduledServiceData newScheduledServiceData = jacsScheduledServiceDataManager.createScheduledService(scheduledServiceData);
        UriBuilder locationURIBuilder = UriBuilder.fromResource(ScheduledServiceInfoResource.class);
        locationURIBuilder.path(newScheduledServiceData.getId().toString());
        return Response
                .created(locationURIBuilder.build())
                .entity(newScheduledServiceData)
                .build();
    }

    @Operation(summary = "List all scheduled services")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Success"),
            @ApiResponse(responseCode = "500", description = "Error occurred")})
    @RequireAuthentication
    @GET
    public Response listAllScheduledServices(@QueryParam("page") Integer pageNumber,
                                             @QueryParam("length") Integer pageLength) {
        PageResult<JacsScheduledServiceData> results = jacsScheduledServiceDataManager.listAllScheduledServices(createPageRequest(pageNumber, pageLength));
        return Response
                .status(Response.Status.OK)
                .entity(results)
                .build();
    }

    private PageRequest createPageRequest(Integer pageNumber, Integer pageLength) {
        PageRequest pageRequest = new PageRequest();
        if (pageNumber != null) {
            pageRequest.setPageNumber(pageNumber);
        }
        if (pageLength != null) {
            pageRequest.setPageSize(pageLength);
        } else {
            pageRequest.setPageSize(DEFAULT_PAGE_SIZE);
        }
        return pageRequest;
    }

    @Operation(summary = "Get service info", description = "Returns service about a given service")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Success"),
            @ApiResponse(responseCode = "500", description = "Error occurred")})
    @RequireAuthentication
    @GET
    @Path("/{service-id}")
    public Response getScheduledService(@PathParam("service-id") Long scheduledServiceId) {
        JacsScheduledServiceData scheduledServiceData = jacsScheduledServiceDataManager.getScheduledServiceById(scheduledServiceId);
        if (scheduledServiceData == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        } else {
            return Response
                    .status(Response.Status.OK)
                    .entity(scheduledServiceData)
                    .build();
        }
    }

    @Operation(summary = "Update scheduled service")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Success"),
            @ApiResponse(responseCode = "404", description = "Entry not found"),
            @ApiResponse(responseCode = "500", description = "Error occurred")})
    @RequireAuthentication
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/{service-id}")
    public Response updateScheduledService(@PathParam("service-id") Long scheduledServiceId,
                                           JacsScheduledServiceData scheduledServiceData) {
        JacsScheduledServiceData existingScheduledServiceData = jacsScheduledServiceDataManager.getScheduledServiceById(scheduledServiceId);
        if (existingScheduledServiceData == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        }
        scheduledServiceData.setId(existingScheduledServiceData.getId());
        JacsScheduledServiceData updatedScheduledServiceData = jacsScheduledServiceDataManager.updateScheduledService(scheduledServiceData);
        return Response
                .status(Response.Status.OK)
                .entity(updatedScheduledServiceData)
                .build();
    }

    @Operation(summary = "Delete scheduled service")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Success"),
            @ApiResponse(responseCode = "404", description = "Entry not found"),
            @ApiResponse(responseCode = "500", description = "Error occurred")})
    @RequireAuthentication
    @DELETE
    @Path("/{service-id}")
    public Response deleteScheduledService(@PathParam("service-id") Long scheduledServiceId) {
        JacsScheduledServiceData existingScheduledServiceData = jacsScheduledServiceDataManager.getScheduledServiceById(scheduledServiceId);
        if (existingScheduledServiceData == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        }
        jacsScheduledServiceDataManager.removeScheduledServiceById(existingScheduledServiceData);
        return Response
                .status(Response.Status.NO_CONTENT)
                .build();
    }

}
