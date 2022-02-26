package org.janelia.jacs2.rest.sync.v2.dataresources;

import io.swagger.annotations.*;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.domain.dao.SyncedPathDao;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.files.SyncedPath;
import org.janelia.model.domain.files.SyncedRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.util.List;

/**
 * Web service for CRUD operations for synced paths.
 */
@SwaggerDefinition(
        securityDefinition = @SecurityDefinition(
                apiKeyAuthDefinitions = {
                        @ApiKeyAuthDefinition(key = "user", name = "username", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER),
                        @ApiKeyAuthDefinition(key = "runAs", name = "runasuser", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER)
                }
        )
)
@Api(
        value = "Janelia Workstation Domain Data",
        authorizations = {
                @Authorization("user"),
                @Authorization("runAs")
        }
)
@RequireAuthentication
@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
@Path("/data")
public class SyncedPathResource {

    private static final Logger LOG = LoggerFactory.getLogger(SyncedPathResource.class);

    @AsyncIndex
    @Inject
    private SyncedPathDao syncedPathDao;

    @ApiOperation(value = "Gets a List of SyncedRoots for the User",
            notes = "Uses the subject key to return a list of SyncedRoots for the user"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully fetched the list of SyncedRoots",  response = SyncedRoot.class,
                    responseContainer = "List" ),
            @ApiResponse( code = 500, message = "Internal Server Error fetching the SyncedRoots" )
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("syncedRoot")
    public Response getSyncedRoots(@ApiParam @QueryParam("subjectKey") String subjectKey) {
        LOG.trace("Start getSyncedRoots({})", subjectKey);
        try {
            List<SyncedRoot> syncedRoots = syncedPathDao.getSyncedRoots(subjectKey);
            return Response
                    .ok(new GenericEntity<List<SyncedRoot>>(syncedRoots){})
                    .build();
        } finally {
            LOG.trace("Finished getSyncedRoots({})", subjectKey);
        }
    }

    @ApiOperation(value = "Creates a SyncedRoot using the DomainObject parameter of the DomainQuery")
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully created a SyncedRoot", response = SyncedRoot.class),
            @ApiResponse( code = 500, message = "Internal Server Error creating a SyncedRoot" )
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("syncedRoot")
    public Response createSyncedRoot(DomainQuery query) {
        LOG.trace("Start createSyncedRoot({})", query);
        try {
            SyncedRoot SyncedRoot = syncedPathDao.createSyncedRoot(query.getSubjectKey(), query.getDomainObjectAs(SyncedRoot.class));
            return Response
                    .created(UriBuilder.fromMethod(this.getClass(), "createSyncedRoot").build(SyncedRoot.getId()))
                    .entity(SyncedRoot)
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred creating a SyncedRoot with {}", query, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error while creating a SyncedRoot from " + query))
                    .build();
        } finally {
            LOG.trace("Finished createSyncedRoot({})", query);
        }
    }

    @ApiOperation(value = "Updates a SyncedRoot using the DomainObject parameter of the DomainQuery")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated a SyncedRoot",
                    response = SyncedRoot.class),
            @ApiResponse(code = 500, message = "Internal Server Error updating a SyncedRoot")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("syncedRoot")
    public Response updateSyncedRoot(DomainQuery query) {
        LOG.trace("Start updateSyncedRoot({})", query);
        try {
            SyncedRoot SyncedRoot = (SyncedRoot)syncedPathDao.saveBySubjectKey(query.getDomainObjectAs(SyncedRoot.class), query.getSubjectKey());
            return Response
                    .ok(SyncedRoot)
                    .contentLocation(UriBuilder.fromMethod(SyncedPathResource.class, "updateSyncedRoot").build(SyncedRoot.getId()))
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred updating the SyncedRoot {}", query, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error while updating the SyncedRoot " + query))
                    .build();
        } finally {
            LOG.trace("Finished updateSyncedRoot({})", query);
        }
    }

    @ApiOperation(value = "Removes the SyncedRoot using the SyncedRoot Id")
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully removed a SyncedRoot"),
            @ApiResponse( code = 500, message = "Internal Server Error removing a SyncedRoot" )
    })
    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Path("syncedRoot")
    public Response removeSyncedRoot(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                     @ApiParam @QueryParam("syncedRootId") final Long syncedRootId) {
        LOG.trace("Start removeSyncedRoot({}, SyncedRootId={})", subjectKey, syncedRootId);
        try {
            SyncedRoot syncedRoot = (SyncedRoot)syncedPathDao.findById(syncedRootId);
            syncedPathDao.removeSyncedRoot(subjectKey, syncedRoot);
            return Response.noContent().build();
        } finally {
            LOG.trace("Finished removeSyncedRoot({}, syncedRootId={})", subjectKey, syncedRootId);
        }
    }

    @ApiOperation(
            value = "Gets the SyncedPath children of a SyncedRoot",
            notes = "Uses the subject key to return a list of children of a SyncedRoot for the user"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully fetched the list of SyncedPaths",  response = SyncedPath.class,
                    responseContainer = "List" ),
            @ApiResponse( code = 500, message = "Internal Server Error list of SyncedPaths" )
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("syncedRoot/children")
    public Response getSyncedRootChildren(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                          @ApiParam @QueryParam("syncedRootId") final Long syncedRootId) {
        LOG.trace("Start getSyncedRootChildren()");
        try {
            SyncedRoot root = (SyncedRoot)syncedPathDao.findById(syncedRootId);
            List<SyncedPath> children = syncedPathDao.getChildren(subjectKey, root, 0, -1);
            return Response
                    .ok(new GenericEntity<List<SyncedPath>>(children){})
                    .build();
        } finally {
            LOG.trace("Finished getSyncedRootChildren()");
        }
    }
}
