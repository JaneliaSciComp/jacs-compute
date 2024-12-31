package org.janelia.jacs2.rest.v2;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;

@ApplicationScoped
@Path("/")
public class StatusResource {
    @GET
    @Produces({"text/plain"})
    public Response getStatus() {
        return Response
                .ok("OK")
                .build();
    }
}
