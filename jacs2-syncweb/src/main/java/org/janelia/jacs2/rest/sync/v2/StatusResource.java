package org.janelia.jacs2.rest.sync.v2;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

@ApplicationScoped
@Produces("application/json")
@Path("/")
public class StatusResource {
    @GET
    public Response getStatus() {
        return Response
                .ok("OK")
                .build();
    }
}
