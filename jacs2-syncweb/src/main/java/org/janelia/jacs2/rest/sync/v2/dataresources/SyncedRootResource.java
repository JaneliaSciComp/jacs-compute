package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.util.List;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.GenericEntity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
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

/**
 * Web service for CRUD operations for synced paths.
 */
@Tag(name = "SyncedRoot",
        description = "Janelia Workstation Domain Data")
@RequireAuthentication
@Produces(MediaType.APPLICATION_JSON)
@Path("/data")
public class SyncedRootResource {

    private static final Logger LOG = LoggerFactory.getLogger(SyncedRootResource.class);

    @AsyncIndex
    @Inject
    private SyncedRootDao syncedRootDao;

    @Inject
    private LegacyDomainDao legacyDomainDao;

    @Operation(summary = "Gets a List of SyncedRoots for the User",
            description = "Uses the subject key to return a list of SyncedRoots for the user"
    )
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully fetched the list of SyncedRoots"),
            @ApiResponse( responseCode = "500", description = "Internal Server Error fetching the SyncedRoots")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("syncedRoot")
    public Response getSyncedRoots(@QueryParam("subjectKey") String subjectKey) {
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

    @Operation(description = "Creates a SyncedRoot using the DomainObject parameter of the DomainQuery")
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully created a SyncedRoot"),
            @ApiResponse( responseCode = "500", description = "Internal Server Error creating a SyncedRoot" )
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

    @Operation(description = "Updates a SyncedRoot using the DomainObject parameter of the DomainQuery")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated a SyncedRoot"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error updating a SyncedRoot")
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

    @Operation(description = "Removes the SyncedRoot using the SyncedRoot Id")
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully removed a SyncedRoot"),
            @ApiResponse( responseCode = "500", description = "Internal Server Error removing a SyncedRoot" )
    })
    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Path("syncedRoot")
    public Response removeSyncedRoot(@QueryParam("subjectKey") final String subjectKey,
                                     @QueryParam("syncedRootId") final Long syncedRootId) {
        LOG.trace("Start removeSyncedRoot({}, SyncedRootId={})", subjectKey, syncedRootId);
        try {
            SyncedRoot syncedRoot = syncedRootDao.findEntityByIdReadableBySubjectKey(syncedRootId, subjectKey);
            syncedRootDao.removeSyncedRoot(subjectKey, syncedRoot);
            return Response.noContent().build();
        } finally {
            LOG.trace("Finished removeSyncedRoot({}, syncedRootId={})", subjectKey, syncedRootId);
        }
    }

    @Operation(
            summary = "Gets the SyncedPath children of a SyncedRoot",
            description = "Uses the subject key to return a list of children of a SyncedRoot for the user"
    )
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully fetched the list of SyncedPaths"),
            @ApiResponse( responseCode = "500", description = "Internal Server Error list of SyncedPaths" )
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("syncedRoot/children")
    public Response getSyncedRootChildren(@QueryParam("subjectKey") final String subjectKey,
                                          @QueryParam("syncedRootId") final Long syncedRootId) {
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
