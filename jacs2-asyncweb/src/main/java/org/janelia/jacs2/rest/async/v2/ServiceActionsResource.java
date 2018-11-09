package org.janelia.jacs2.rest.async.v2;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.janelia.jacs2.asyncservice.JacsServiceDataManager;
import org.janelia.jacs2.asyncservice.JacsServiceEngine;
import org.janelia.jacs2.auth.JacsServiceAccessDataUtils;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceState;
import org.slf4j.Logger;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

@RequestScoped
@Produces("application/json")
@Path("/service-actions")
@Api(value = "JACS Service Info")
public class ServiceActionsResource {

    @Inject private Logger logger;
    @Inject private JacsServiceDataManager jacsServiceDataManager;
    @Inject private JacsServiceEngine jacsServiceEngine;

    @RequireAuthentication
    @PUT
    @Path("/{service-instance-id}/update")
    @ApiOperation(value = "Update service data", notes = "Updates the specified service data")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    public Response updateServiceInfo(@PathParam("service-instance-id") Long instanceId,
                                      JacsServiceData si,
                                      @Context SecurityContext securityContext) {
        JacsServiceData serviceData = jacsServiceDataManager.retrieveServiceById(instanceId);
        if (serviceData == null) {
            logger.warn("No service found for {}", instanceId);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        }
        if (JacsServiceAccessDataUtils.canServiceBeModifiedBy(serviceData, securityContext)) {
            JacsServiceData updatedServiceData = jacsServiceDataManager.updateService(instanceId, si);
            return Response
                    .status(Response.Status.OK)
                    .entity(updatedServiceData)
                    .build();
        } else {
            logger.warn("Service {} cannot be modified by {}", serviceData, securityContext.getUserPrincipal().getName());
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .build();
        }
    }

    @RequireAuthentication
    @PUT
    @Path("/{service-instance-id}/state-update/{service-state}")
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
        if (JacsServiceAccessDataUtils.canServiceBeModifiedBy(serviceData, securityContext)) {
            JacsServiceData updatedServiceData = jacsServiceEngine.updateServiceState(serviceData, serviceState);
            return Response
                    .status(Response.Status.OK)
                    .entity(updatedServiceData)
                    .build();
        } else {
            logger.warn("Service state {} cannot be modified by {}", serviceData, securityContext.getUserPrincipal().getName());
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .build();
        }
    }

}
