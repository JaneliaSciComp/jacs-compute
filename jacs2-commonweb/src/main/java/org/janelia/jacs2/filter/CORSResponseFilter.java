package org.janelia.jacs2.filter;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import java.io.IOException;

public class CORSResponseFilter implements ContainerResponseFilter {
    @Override
    public void filter(ContainerRequestContext request, ContainerResponseContext resp) throws IOException {
        resp.getHeaders().add("Access-Control-Allow-Origin","*");
        resp.getHeaders().add("Access-Control-Allow-Methods","GET,POST,PUT,DELETE");
        resp.getHeaders().add("Access-Control-Allow-Headers","Origin, X-Requested-With, Content-Type, Accept, Authorization, Application-Id, Content-Disposition");
    }
}
