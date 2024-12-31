package org.janelia.jacs2.rest.async.v2;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.janelia.jacs2.asyncservice.JacsServiceDataManager;
import org.janelia.jacs2.asyncservice.JacsServiceEngine;
import org.janelia.jacs2.auth.JacsServiceAccessDataUtils;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceState;
import org.slf4j.Logger;

@Tag(name = "ServiceActions", description = "JACS Service Info")
@RequestScoped
@Produces("application/json")
@Path("/services")
public class ServiceActionsResource {

    @Inject private Logger logger;
    @Inject private JacsServiceDataManager jacsServiceDataManager;
    @Inject private JacsServiceEngine jacsServiceEngine;

    @Operation(summary = "Update service data", description = "Updates the specified service data")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Success"),
            @ApiResponse(responseCode = "500", description = "Error occurred") })
    @RequireAuthentication
    @PUT
    @Path("/{service-instance-id}")
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

    @Operation(
            summary = "Update service state",
            description = "Updates the state of the given service. " +
                    "This endpoint can be used for terminating, suspending or resuming a service. " +
                    "The respective values for terminating, suspending, and resuming a service are: " +
                    "CANCELED, SUSPENDED, and RESUMED.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Success"),
            @ApiResponse(responseCode = "404", description = "If the service state or the transition is invalid"),
            @ApiResponse(responseCode = "500", description = "Error occurred") })
    @RequireAuthentication
    @PUT
    @Path("/{service-instance-id}/state/{service-state}")
    public Response updateServiceState(@PathParam("service-instance-id") Long instanceId,
                                       @PathParam("service-state") JacsServiceState serviceState,
                                       @QueryParam("force") Boolean force,
                                       @Context SecurityContext securityContext) {
        JacsServiceData serviceData = jacsServiceDataManager.retrieveServiceById(instanceId);
        if (serviceData == null) {
            logger.warn("No service found for {}", instanceId);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        }
        if (JacsServiceAccessDataUtils.canServiceBeModifiedBy(serviceData, securityContext)) {
            boolean forceFlag = force != null && force;
            JacsServiceData updatedServiceData = jacsServiceEngine.updateServiceState(serviceData, serviceState, forceFlag);
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
