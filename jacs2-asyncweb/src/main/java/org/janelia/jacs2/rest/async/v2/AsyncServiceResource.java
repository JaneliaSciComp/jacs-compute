package org.janelia.jacs2.rest.async.v2;

import java.util.List;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.core.UriBuilder;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.JacsServiceEngine;
import org.janelia.jacs2.asyncservice.ServerStats;
import org.janelia.jacs2.auth.JacsSecurityContext;
import org.janelia.jacs2.auth.JacsSecurityContextHelper;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.model.domain.enums.SubjectRole;
import org.janelia.model.service.JacsServiceData;

@Tag(name = "AsyncService", description = "Asynchronous JACS Service API")
@Produces(MediaType.APPLICATION_JSON)
@Path("/async-services")
public class AsyncServiceResource {

    @Inject
    private JacsServiceEngine jacsServiceEngine;

    @Operation(summary = "Submit a list of services", description = "The submission assumes an implicit positional dependecy where each service depends on its predecessors")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "Success"),
            @ApiResponse(responseCode = "500", description = "Error occurred")})
    @RequireAuthentication
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createAsyncServices(List<JacsServiceData> services, @Context ContainerRequestContext containerRequestContext) {
        String authenticatedSubjectKey = JacsSecurityContextHelper.getAuthenticatedSubjectKey(containerRequestContext);
        String authorizedSubjectKey = JacsSecurityContextHelper.getAuthorizedSubjectKey(containerRequestContext);
        services.forEach((service) -> {
            service.setAuthKey(authenticatedSubjectKey);
            service.setOwnerKey(authorizedSubjectKey);
            for (String requestHeader : containerRequestContext.getHeaders().keySet()) {
                service.addDictionaryArg(requestHeader, containerRequestContext.getHeaderString(requestHeader));
            }
        }); // update the owner for all submitted services
        List<JacsServiceData> newServices = jacsServiceEngine.submitMultipleServices(services);
        return Response
                .status(Response.Status.CREATED)
                .entity(newServices)
                .build();
    }

    @Operation(summary = "Submit a single service of the specified type")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "Success"),
            @ApiResponse(responseCode = "500", description = "Error occurred")})
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
        for (String requestHeader : containerRequestContext.getHeaders().keySet()) {
            si.addDictionaryArg(requestHeader, containerRequestContext.getHeaderString(requestHeader));
        }
        JacsServiceData newJacsServiceData = jacsServiceEngine.submitSingleService(si);
        return Response
                .created(UriBuilder.fromMethod(ServiceInfoResource.class, "getServiceInfo").build(newJacsServiceData.getId()))
                .entity(newJacsServiceData)
                .build();
    }

    @Operation(summary = "Update the number of processing slots", description = "A value of 0 disables the processing of new services")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Success"),
            @ApiResponse(responseCode = "403", description = "If the user doesn't have admin privileges"),
            @ApiResponse(responseCode = "500", description = "Error occurred")})
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

    @Operation(summary = "Update the size of the waiting queue")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Success"),
            @ApiResponse(responseCode = "403", description = "If the user doesn't have admin privileges"),
            @ApiResponse(responseCode = "500", description = "Error occurred")})
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

    @Operation(summary = "Retrieve processing statistics")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Success"),
            @ApiResponse(responseCode = "500", description = "Error occurred")})
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
