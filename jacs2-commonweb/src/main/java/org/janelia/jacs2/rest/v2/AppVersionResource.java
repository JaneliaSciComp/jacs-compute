package org.janelia.jacs2.rest.v2;

import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.janelia.jacs2.asyncservice.JacsServiceDataManager;
import org.janelia.jacs2.asyncservice.JacsServiceEngine;
import org.janelia.jacs2.asyncservice.ServerStats;
import org.janelia.jacs2.auth.JacsSecurityContextHelper;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.domain.enums.SubjectRole;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceState;
import org.slf4j.Logger;

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
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;

@RequestScoped
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
