package org.janelia.jacs2.rest.async.v2;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.dataservice.cronservice.CronScheduledServiceManager;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.service.JacsScheduledServiceData;
import org.slf4j.Logger;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

@Api(value = "Scheduled JACS Services")
@RequestScoped
@Produces("application/json")
@Path("/scheduled-services")
public class ScheduledServiceInfoResource {
    private static final int DEFAULT_PAGE_SIZE = 100;

    @Inject private CronScheduledServiceManager jacsScheduledServiceDataManager;

    @ApiOperation(value = "Create a scheduled service entry")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
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

    @ApiOperation(value = "List all scheduled services", notes = "")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
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

    @ApiOperation(value = "Get service info", notes = "Returns service about a given service")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
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

    @ApiOperation(value = "Update scheduled service")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 404, message = "Entry not found"),
            @ApiResponse(code = 500, message = "Error occurred") })
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

    @ApiOperation(value = "Delete scheduled service")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Success"),
            @ApiResponse(code = 404, message = "Entry not found"),
            @ApiResponse(code = 500, message = "Error occurred") })
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
