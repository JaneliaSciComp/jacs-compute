package org.janelia.jacs2.rest.async.v2;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.JacsServiceDataManager;
import org.janelia.jacs2.asyncservice.ServiceRegistry;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.dataservice.cronservice.CronScheduledService;
import org.janelia.model.domain.enums.SubjectRole;
import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.service.JacsScheduledServiceData;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceState;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;
import java.util.function.Function;

@RequestScoped
@Produces("application/json")
@Path("/scheduled-services")
@Api(value = "Scheduled JACS Services")
public class ScheduledServiceInfoResource {
    private static final int DEFAULT_PAGE_SIZE = 100;

    @Inject private Logger logger;
    @Inject private CronScheduledService jacsScheduledServiceDataManager;

    @RequireAuthentication
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a scheduled service entry")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    public Response createScheduledService(JacsScheduledServiceData scheduledServiceData) {
        JacsScheduledServiceData newScheduledServiceData = jacsScheduledServiceDataManager.createScheduledService(scheduledServiceData);
        UriBuilder locationURIBuilder = UriBuilder.fromResource(ScheduledServiceInfoResource.class);
        locationURIBuilder.path(newScheduledServiceData.getId().toString());
        return Response
                .created(locationURIBuilder.build())
                .entity(newScheduledServiceData)
                .build();
    }

    @RequireAuthentication
    @GET
    @ApiOperation(value = "List all scheduled services", notes = "")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
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

    @RequireAuthentication
    @GET
    @Path("/{service-id}")
    @ApiOperation(value = "Get service info", notes = "Returns service about a given service")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
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

    @RequireAuthentication
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/{service-id}")
    @ApiOperation(value = "Update scheduled service")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
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

}
