package org.janelia.jacs2.rest.sync.v2.dataresources;

import io.swagger.annotations.*;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.SyncedRootDao;
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
import java.util.stream.Collectors;

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
public class SyncedRootResource {

    private static final Logger LOG = LoggerFactory.getLogger(SyncedRootResource.class);

    @AsyncIndex
    @Inject
    private SyncedRootDao syncedRootDao;

    @Inject
    private LegacyDomainDao legacyDomainDao;

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
            List<SyncedRoot> syncedRoots = syncedRootDao.getSyncedRoots(subjectKey);
            return Response
                    .ok(new GenericEntity<List<SyncedRoot>>(syncedRoots){})
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred getting SyncedRoots for {}", subjectKey, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error getting SyncedRoots for " + subjectKey))
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
            SyncedRoot syncedRoot = query.getDomainObjectAs(SyncedRoot.class);
            SyncedRoot SyncedRoot = syncedRootDao.createSyncedRoot(syncedRoot.getOwnerKey(), syncedRoot);
            if (!query.getSubjectKey().equals(syncedRoot.getOwnerKey())) {
                // Share the object back to the creating user
                syncedRoot.getReaders().add(query.getSubjectKey());
                syncedRoot.getWriters().add(query.getSubjectKey());
                legacyDomainDao.addPermissions(syncedRoot.getOwnerKey(), SyncedRoot.class.getName(),
                        syncedRoot.getId(), syncedRoot, false);
            }
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
            SyncedRoot SyncedRoot = syncedRootDao.saveBySubjectKey(query.getDomainObjectAs(SyncedRoot.class), query.getSubjectKey());
            return Response
                    .ok(SyncedRoot)
                    .contentLocation(UriBuilder.fromMethod(SyncedRootResource.class, "updateSyncedRoot").build(SyncedRoot.getId()))
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
            SyncedRoot syncedRoot = syncedRootDao.findEntityByIdReadableBySubjectKey(syncedRootId, subjectKey);
            syncedRootDao.removeSyncedRoot(subjectKey, syncedRoot);
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
            SyncedRoot syncedRoot = syncedRootDao.findById(syncedRootId);
            List<SyncedPath> children = legacyDomainDao.getDomainObjects(subjectKey, syncedRoot.getChildren())
                    .stream().map(d -> (SyncedPath) d).collect(Collectors.toList());
            return Response
                    .ok(new GenericEntity<List<SyncedPath>>(children){})
                    .build();
        } finally {
            LOG.trace("Finished getSyncedRootChildren()");
        }
    }
}
