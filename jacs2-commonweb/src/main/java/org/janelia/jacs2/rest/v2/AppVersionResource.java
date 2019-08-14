package org.janelia.jacs2.rest.v2;

import java.io.InputStream;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import com.google.common.io.ByteStreams;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.janelia.jacs2.rest.ErrorResponse;
import org.slf4j.Logger;

@ApplicationScoped
@Path("/version")
@Api(value = "Application version API")
public class AppVersionResource {

    @Inject private Logger logger;

    @ApiOperation(value = "Get application's version")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    @GET
    @Produces({"text/plain", "application/json"})
    public Response getApplicationVersion() {
        try (InputStream configStream = this.getClass().getResourceAsStream("/version.txt")) {
            String version = new String(ByteStreams.toByteArray(configStream));
            return Response
                    .status(Response.Status.OK)
                    .entity(version.trim())
                    .build();
        } catch (Exception e) {
            logger.error("Error reading version.txt", e);
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse(e.getMessage()))
                    .build();
        }
    }

}
