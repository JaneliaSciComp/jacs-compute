package org.janelia.jacs2.rest.sync.v2.dataresources.search;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.collections4.CollectionUtils;
import org.glassfish.jersey.process.JerseyProcessingUncaughtExceptionHandler;
import org.glassfish.jersey.server.ManagedAsync;
import org.janelia.jacs2.auth.JacsSecurityContextHelper;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.dataservice.search.IndexingService;
import org.janelia.model.domain.Reference;
import org.janelia.model.security.Group;
import org.janelia.model.security.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

@Api(value = "Janelia Workstation Domain Data")
@Path("/data")
public class DomainIndexingResource {
    private static final Logger LOG = LoggerFactory.getLogger(DomainIndexingResource.class);

    @Inject
    private IndexingService indexingService;

    private static final ExecutorService ASYNC_TASK_EXECUTOR = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
            .setNameFormat("JACS-INDEXING-%d")
            .setDaemon(true)
            .build());

    @ApiOperation(value = "Add document to index")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully added new document to SOLR index"),
            @ApiResponse(code = 500, message = "Internal Server Error while adding document to SOLR index")
    })
    @POST
    @Path("searchIndex")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response indexDocuments(@ApiParam List<Reference> domainObjectReferences) {
        LOG.trace("Start indexDocument({})", domainObjectReferences);
        try {
            indexingService.indexDocuments(domainObjectReferences);
            return Response.ok().build();
        } catch (Exception e) {
            LOG.error("Error occurred while adding {} to index", domainObjectReferences, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } finally {
            LOG.trace("Finished indexDocument({})", domainObjectReferences);
        }
    }

    @ApiOperation(value = "Update the ancestor ids for the list of childrens specified in the body of the request")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully performed SOLR index update"),
            @ApiResponse(code = 500, message = "Internal Server Error performing SOLR index update")
    })
    @PUT
    @Path("searchIndex/{docId}/descendants")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateDocDescendants(@PathParam("docId") Long ancestorId, @ApiParam List<Long> descendantIds) {
        LOG.trace("Start addAncestorIdToAllDocs({}, {})", ancestorId, descendantIds);
        try {
            if (CollectionUtils.isNotEmpty(descendantIds)) {
                indexingService.updateDocsAncestors(ImmutableSet.copyOf(descendantIds), ancestorId);
            }
            return Response.ok().build();
        } catch (Exception e) {
            LOG.error("Error occurred while updating the ancestors for {} to {}", descendantIds, ancestorId, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } finally {
            LOG.trace("Finished addAncestorIdToAllDocs({}, {})", ancestorId, descendantIds);
        }
    }

    @RequireAuthentication
    @ApiOperation(value = "Refresh the entire search index. " +
            "The operation requires admin privileges because it may require a lot of resources to perform the action " +
            "and I don't want to let anybody to just go and refresh the index")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully performed SOLR index update"),
            @ApiResponse(code = 403, message = "The authorized subject is not an admin"),
            @ApiResponse(code = 500, message = "Internal Server Error performing SOLR index clear")
    })
    @ManagedAsync
    @PUT
    @Path("searchIndex")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public void refreshDocumentsIndex(@QueryParam("clearIndex") Boolean clearIndex,
                                      @Context ContainerRequestContext containerRequestContext,
                                      @Suspended AsyncResponse asyncResponse) {
        ASYNC_TASK_EXECUTOR.submit(() -> {
            LOG.trace("Start updateAllDocumentsIndex()");
            try {
                User authorizedSubject = JacsSecurityContextHelper.getAuthorizedUser(containerRequestContext);
                if (authorizedSubject == null) {
                    LOG.warn("Unauthorized attempt to update the entire search index");
                    asyncResponse.resume(new WebApplicationException(Response.Status.FORBIDDEN));
                    return;
                }
                if (!authorizedSubject.hasGroupWrite(Group.ADMIN_KEY)) {
                    LOG.warn("Non-admin user {} attempted to update the entire search index", authorizedSubject.getName());
                    asyncResponse.resume(new WebApplicationException(Response.Status.FORBIDDEN));
                    return;
                }
                int nDocs = indexingService.indexAllDocuments(clearIndex != null && clearIndex);
                asyncResponse.resume(nDocs);
            } catch (Exception e) {
                LOG.error("Error occurred while deleting document index", e);
                asyncResponse.resume(e);
            } finally {
                LOG.trace("Finished updateAllDocumentsIndex()");
            }
        });
    }

    @RequireAuthentication
    @ApiOperation(value = "Clear search index. The operation requires admin privileges")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully performed SOLR index clear"),
            @ApiResponse(code = 403, message = "The authorized subject is not an admin"),
            @ApiResponse(code = 500, message = "Internal Server Error performing SOLR index clear")
    })
    @DELETE
    @Path("searchIndex")
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeDocumentsIndex(@Context ContainerRequestContext containerRequestContext) {
        LOG.trace("Start deleteDocumentsIndex()");
        try {
            User authorizedSubject = JacsSecurityContextHelper.getAuthorizedUser(containerRequestContext);
            if (authorizedSubject == null) {
                LOG.warn("Unauthorized attempt to delete the search index");
                return Response.status(Response.Status.FORBIDDEN).build();
            }
            if (!authorizedSubject.hasGroupWrite(Group.ADMIN_KEY)) {
                LOG.warn("Non-admin user {} attempted to remove search index", authorizedSubject.getName());
                return Response.status(Response.Status.FORBIDDEN).build();
            }
            indexingService.removeIndex();
            return Response.noContent().build();
        } catch (Exception e) {
            LOG.error("Error occurred while deleting document index", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } finally {
            LOG.trace("Finished deleteDocumentsIndex()");
        }
    }

    @ApiOperation(value = "Delete documents from index.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully performed SOLR index clear"),
            @ApiResponse(code = 500, message = "Internal Server Error performing SOLR index clear")
    })
    @POST
    @Path("searchIndex/docsToRemove")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteDocumentsFromIndex(List<Long> docIds) {
        LOG.trace("Start deleteDocumentsFromIndex({})", docIds);
        try {
            indexingService.removeDocuments(docIds);
            return Response.noContent().build();
        } catch (Exception e) {
            LOG.error("Error occurred while deleting documents {} index", docIds, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } finally {
            LOG.trace("Finished deleteDocumentsFromIndex({})", docIds);
        }
    }

}
