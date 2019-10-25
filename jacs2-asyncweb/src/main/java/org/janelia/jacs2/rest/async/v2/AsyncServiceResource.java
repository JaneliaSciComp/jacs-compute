package org.janelia.jacs2.rest.async.v2;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.JacsServiceEngine;
import org.janelia.jacs2.asyncservice.ServerStats;
import org.janelia.jacs2.auth.JacsSecurityContext;
import org.janelia.jacs2.auth.JacsSecurityContextHelper;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.model.domain.enums.SubjectRole;
import org.janelia.model.security.Subject;
import org.janelia.model.service.JacsServiceData;

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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriBuilder;
import java.util.List;

@Api(value = "Asynchronous JACS Service API")
@RequestScoped
@Produces(MediaType.APPLICATION_JSON)
@Path("/async-services")
public class AsyncServiceResource {

    @Inject private JacsServiceEngine jacsServiceEngine;

    @ApiOperation(value = "Submit a list of services", notes = "The submission assumes an implicit positional dependecy where each service depends on its predecessors")
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    @RequireAuthentication
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createAsyncServices(List<JacsServiceData> services, @Context ContainerRequestContext containerRequestContext) {
        String authenticatedSubjectKey = JacsSecurityContextHelper.getAuthenticatedSubjectKey(containerRequestContext);
        String authorizedSubjectKey = JacsSecurityContextHelper.getAuthorizedSubjectKey(containerRequestContext);
        services.forEach((service) -> {
            service.setAuthKey(authenticatedSubjectKey);
            service.setOwnerKey(authorizedSubjectKey);
        }); // update the owner for all submitted services
        List<JacsServiceData> newServices = jacsServiceEngine.submitMultipleServices(services);
        return Response
                .status(Response.Status.CREATED)
                .entity(newServices)
                .build();
    }

    @ApiOperation(value = "Submit a single service of the specified type", notes = "")
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    @RequireAuthentication
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/{service-name}")
    public Response createAsyncService(@PathParam("service-name") String serviceName, JacsServiceData si, @Context ContainerRequestContext containerRequestContext) {
        JacsSecurityContext securityContext = JacsSecurityContextHelper.getSecurityContext(containerRequestContext);
        String authenticatedSubjectKey = securityContext.getAuthenticatedSubjectKey();
        String authorizedSubjectKey = securityContext.getAuthorizedSubjectKey();
        si.setAuthKey(authenticatedSubjectKey);
        if (securityContext.hasAdminPrivileges()) {
            // if the authenticated user has admin privileges
            // let him set a different owner
            if (StringUtils.isBlank(si.getOwnerKey())) {
                si.setOwnerKey(authorizedSubjectKey);
            }
        } else {
            si.setOwnerKey(authenticatedSubjectKey);
        }
        si.setName(serviceName);
        JacsServiceData newJacsServiceData = jacsServiceEngine.submitSingleService(si);
        return Response
                .created(UriBuilder.fromMethod(ServiceInfoResource.class, "getServiceInfo").build(newJacsServiceData.getId()))
                .entity(newJacsServiceData)
                .build();
    }

    @ApiOperation(value = "Update the number of processing slots", notes = "A value of 0 disables the processing of new services")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 403, message = "If the user doesn't have admin privileges"),
            @ApiResponse(code = 500, message = "Error occurred") })
    @RequireAuthentication
    @PUT
    @Path("/processing-slots-count/{slots-count}")
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

    @ApiOperation(value = "Update the size of the waiting queue", notes = "")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 403, message = "If the user doesn't have admin privileges"),
            @ApiResponse(code = 500, message = "Error occurred") })
    @RequireAuthentication
    @PUT
    @Path("/waiting-slots-count/{slots-count}")
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

    @ApiOperation(value = "Retrieve processing statistics", notes = "")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    @RequireAuthentication
    @GET
    @Path("/stats")
    public Response getServerStats() {
        ServerStats stats = jacsServiceEngine.getServerStats();
        return Response
                .status(Response.Status.OK)
                .entity(stats)
                .build();
    }

}
