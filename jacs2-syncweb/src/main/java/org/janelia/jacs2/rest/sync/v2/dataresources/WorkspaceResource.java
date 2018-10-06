package org.janelia.jacs2.rest.sync.v2.dataresources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.domain.workspace.Workspace;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Api(value = "Workspace Data Service")
@RequireAuthentication
@ApplicationScoped
@Produces("application/json")
@Path("/data")
public class WorkspaceResource {

    @Inject private LegacyDomainDao legacyWorkspaceDao;
    @Inject private Logger logger;

    @ApiOperation(value = "Gets all the Workspaces a user can read",
            notes = "Returns all the Workspaces which are visible to the current user."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = 200, message = "Successfully got all workspaces",
                    response = Workspace.class,
                    responseContainer =  "List"),
            @ApiResponse( code = 500, message = "Internal Server Error getting workspaces" )
    })
    @GET
    @Path("/workspaces")
    @Produces(MediaType.APPLICATION_JSON)
    public List<Workspace> getAllWorkspaces(@QueryParam("subjectKey") String subjectKey) {
        logger.trace("Start getAllWorkspace({})",subjectKey);
        try {
            return legacyWorkspaceDao.getWorkspaces(subjectKey);
        } catch (Exception e) {
            logger.error("Error occurred getting default workspace for {}", subjectKey, e);
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            logger.trace("Finished getAllWorkspace({})", subjectKey);
        }
    }

}
