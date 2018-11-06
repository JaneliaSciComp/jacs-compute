package org.janelia.jacs2.rest.async.v2;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.janelia.jacs2.asyncservice.JacsServiceDataManager;
import org.janelia.jacs2.asyncservice.JacsServiceEngine;
import org.janelia.jacs2.asyncservice.ServerStats;
import org.janelia.jacs2.asyncservice.common.ResourceHelper;
import org.janelia.jacs2.auth.JacsSecurityContextHelper;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.model.domain.enums.SubjectRole;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceState;
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
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriBuilder;
import java.util.List;

@RequestScoped
@Produces(MediaType.APPLICATION_JSON)
@Path("/async-services")
@Api(value = "Asynchronous JACS Service API")
public class AsyncServiceResource {

    @Inject private Logger logger;
    @Inject private JacsServiceEngine jacsServiceEngine;
    @Inject private JacsServiceDataManager jacsServiceDataManager;

    @RequireAuthentication
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Submit a list of services", notes = "The submission assumes an implicit positional dependecy where each service depends on its predecessors")
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    public Response createAsyncServices(List<JacsServiceData> services, @Context ContainerRequestContext containerRequestContext) {
        String authenticatedSubject = JacsSecurityContextHelper.getAuthenticatedSubjectKey(containerRequestContext);
        String authorizedSubject = JacsSecurityContextHelper.getAuthorizedSubjectKey(containerRequestContext);
        services.forEach((service) -> {
            service.setAuthKey(authenticatedSubject);
            service.setOwnerKey(authorizedSubject);
        }); // update the owner for all submitted services
        List<JacsServiceData> newServices = jacsServiceEngine.submitMultipleServices(services);
        return Response
                .status(Response.Status.CREATED)
                .entity(newServices)
                .build();
    }

    @RequireAuthentication
    @POST
    @Path("/{service-name}")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Submit a single service of the specified type", notes = "")
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    public Response createAsyncService(@PathParam("service-name") String serviceName, JacsServiceData si, @Context ContainerRequestContext containerRequestContext) {
        String authenticatedSubjectKey = JacsSecurityContextHelper.getAuthenticatedSubjectKey(containerRequestContext);
        String authorizedSubjectKey = JacsSecurityContextHelper.getAuthorizedSubjectKey(containerRequestContext);
        si.setAuthKey(authenticatedSubjectKey);
        si.setOwnerKey(authorizedSubjectKey);
        si.setName(serviceName);
        JacsServiceData newJacsServiceData = jacsServiceEngine.submitSingleService(si);
        UriBuilder locationURIBuilder = UriBuilder.fromResource(ServiceInfoResource.class);
        locationURIBuilder.path(newJacsServiceData.getId().toString());
        return Response
                .status(Response.Status.CREATED)
                .entity(newJacsServiceData)
                .contentLocation(locationURIBuilder.build())
                .build();
    }

    @RequireAuthentication
    @PUT
    @Path("/{service-instance-id}/state/{service-state}")
    @ApiOperation(
            value = "Update service state",
            notes = "Updates the state of the given service. " +
                    "This endpoint can be used for terminating, suspending or resuming a service. " +
                    "The respective values for terminating, suspending, and resuming a service are: " +
                    "CANCELED, SUSPENDED, and RESUMED.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 404, message = "If the service state or the transition is invalid"),
            @ApiResponse(code = 500, message = "Error occurred") })
    public Response updateServiceState(@PathParam("service-instance-id") Long instanceId,
                                       @PathParam("service-state") JacsServiceState serviceState,
                                       @Context SecurityContext securityContext) {
        JacsServiceData serviceData = jacsServiceDataManager.retrieveServiceById(instanceId);
        if (serviceData == null) {
            logger.warn("No service found for {}", instanceId);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        }
        if (serviceData.canBeModifiedBy(securityContext.getUserPrincipal().getName())) {
            serviceData = jacsServiceEngine.updateServiceState(serviceData, serviceState);
            return Response
                    .status(Response.Status.OK)
                    .entity(serviceData)
                    .build();
        } else {
            logger.warn("Service state {} cannot be modified by {}", serviceData, securityContext.getUserPrincipal().getName());
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .build();
        }
    }

    @RequireAuthentication
    @PUT
    @Path("/processing-slots-count/{slots-count}")
    @ApiOperation(value = "Update the number of processing slots", notes = "A value of 0 disables the processing of new services")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 403, message = "If the user doesn't have admin privileges"),
            @ApiResponse(code = 500, message = "Error occurred") })
    public Response setProcessingSlotsCount(@PathParam("slots-count") int nProcessingSlots, @Context SecurityContext securityContext) {
        if (!securityContext.isUserInRole(SubjectRole.Admin.getRole())) {
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .build();
        }
        jacsServiceEngine.setProcessingSlotsCount(nProcessingSlots);
        return Response
                .status(Response.Status.OK)
                .build();
    }

    @RequireAuthentication
    @PUT
    @Path("/waiting-slots-count/{slots-count}")
    @ApiOperation(value = "Update the size of the waiting queue", notes = "")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 403, message = "If the user doesn't have admin privileges"),
            @ApiResponse(code = 500, message = "Error occurred") })
    public Response setWaitingSlotsCount(@PathParam("slots-count") int nWaitingSlots, @Context SecurityContext securityContext) {
        if (!securityContext.isUserInRole(SubjectRole.Admin.getRole())) {
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .build();
        }
        jacsServiceEngine.setMaxWaitingSlots(nWaitingSlots);
        return Response
                .status(Response.Status.OK)
                .build();
    }

    @RequireAuthentication
    @GET
    @Path("/stats")
    @ApiOperation(value = "Retrieve processing statistics", notes = "")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    public Response getServerStats() {
        ServerStats stats = jacsServiceEngine.getServerStats();
        return Response
                .status(Response.Status.OK)
                .entity(stats)
                .build();
    }

}
