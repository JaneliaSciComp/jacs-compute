package org.janelia.jacs2.rest.async.v2;

import org.janelia.jacs2.asyncservice.JacsServiceEngine;
import org.janelia.jacs2.asyncservice.ServerStats;
import org.janelia.jacs2.auth.AuthManager;
import org.janelia.model.service.JacsServiceData;
import org.slf4j.Logger;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.util.List;

@RequestScoped
@Produces("application/json")
@Path("/async-services")
public class AsyncServiceInfoResource {

    @Inject private Logger log;
    @Inject private JacsServiceEngine jacsServiceEngine;
    @Inject private AuthManager authManager;

    @POST
    @Consumes("application/json")
    public Response createAsyncServices(List<JacsServiceData> services) {
        String subjectKey = authManager.authorize();
        services.stream().forEach((service) -> service.setOwner(subjectKey));
        List<JacsServiceData> newServices = jacsServiceEngine.submitMultipleServices(services);
        return Response
                .status(Response.Status.CREATED)
                .entity(newServices)
                .build();
    }

    @POST
    @Path("/{service-name}")
    @Consumes("application/json")
    public Response createAsyncService(@PathParam("service-name") String serviceName, JacsServiceData si) {
        String subjectKey = authManager.authorize();
        si.setOwner(subjectKey);
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

    @GET
    @Path("/stats")
    public Response getServerStats() {
        authManager.authorize();
        ServerStats stats = jacsServiceEngine.getServerStats();
        return Response
                .status(Response.Status.OK)
                .entity(stats)
                .build();
    }

    @PUT
    @Path("/processing-slots-count/{slots-count}")
    public Response setProcessingSlotsCount(@PathParam("slots-count") int nProcessingSlots) {
        authManager.authorize();
        jacsServiceEngine.setProcessingSlotsCount(nProcessingSlots);
        return Response
                .status(Response.Status.OK)
                .build();
    }

    @PUT
    @Path("/waiting-slots-count/{slots-count}")
    public Response setWaitingSlotsCount(@PathParam("slots-count") int nWaitingSlots) {
        authManager.authorize();
        jacsServiceEngine.setMaxWaitingSlots(nWaitingSlots);
        return Response
                .status(Response.Status.OK)
                .build();
    }

}
