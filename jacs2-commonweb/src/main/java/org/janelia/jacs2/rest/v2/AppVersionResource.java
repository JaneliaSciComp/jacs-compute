package org.janelia.jacs2.rest.v2;

import java.io.InputStream;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;

import com.google.common.io.ByteStreams;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.janelia.jacs2.rest.ErrorResponse;
import org.slf4j.Logger;

@ApplicationScoped
@Path("/version")
@Tag(name = "AppVersion", description = "Application version API")
public class AppVersionResource {

    @Inject private Logger logger;

    @Operation(summary = "Get application's version")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Success"),
            @ApiResponse(responseCode = "500", description = "Error occurred") })
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
