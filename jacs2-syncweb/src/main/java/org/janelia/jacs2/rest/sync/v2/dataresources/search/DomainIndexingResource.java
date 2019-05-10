package org.janelia.jacs2.rest.sync.v2.dataresources.search;

import com.google.common.collect.ImmutableSet;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacs2.auth.JacsSecurityContextHelper;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.dataservice.search.IndexingService;
import org.janelia.model.domain.DomainObject;
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
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Api(value = "Janelia Workstation Domain Data")
@RequireAuthentication
@Path("/data")
public class DomainIndexingResource {
    private static final Logger LOG = LoggerFactory.getLogger(DomainIndexingResource.class);

    @Inject
    private IndexingService indexingService;

    @ApiOperation(value = "Add document to index")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully added new document to SOLR index"),
            @ApiResponse(code = 500, message = "Internal Server Error while adding document to SOLR index")
    })
    @POST
    @Path("searchIndex")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response indexDocument(@ApiParam Reference domainObjectReference, @Context ContainerRequestContext containerRequestContext) {
        LOG.trace("Start indexDocument({})", domainObjectReference);
        try {
            User authorizedSubject = JacsSecurityContextHelper.getAuthorizedUser(containerRequestContext);
            indexingService.indexDocument(authorizedSubject != null ? authorizedSubject.getKey() : null, domainObjectReference);
            return Response.ok(domainObjectReference).build();
        } catch (Exception e) {
            LOG.error("Error occurred while adding {} to index", domainObjectReference, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } finally {
            LOG.trace("Finished indexDocument({})", domainObjectReference);
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

    @ApiOperation(value = "Refresh the entire search index. " +
            "The operation requires admin privileges because it may require a lot of resources to perform the action " +
            "and I don't want to let anybody to just go and refresh the index")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully performed SOLR index update"),
            @ApiResponse(code = 403, message = "The authorized subject is not an admin"),
            @ApiResponse(code = 500, message = "Internal Server Error performing SOLR index clear")
    })
    @PUT
    @Path("searchIndex")
    @Produces(MediaType.APPLICATION_JSON)
    public Response refreshDocumentsIndex(@QueryParam("clearIndex") Boolean clearIndex, @Context ContainerRequestContext containerRequestContext) {
        LOG.trace("Start updateAllDocumentsIndex()");
        try {
            User authorizedSubject = JacsSecurityContextHelper.getAuthorizedUser(containerRequestContext);
            if (authorizedSubject == null) {
                LOG.warn("Unauthorized attempt to update the entire search index");
                return Response.status(Response.Status.FORBIDDEN).build();
            }
            if (!authorizedSubject.hasGroupWrite(Group.ADMIN_KEY)) {
                LOG.warn("Non-admin user {} attempted to update the entire search index", authorizedSubject.getName());
                return Response.status(Response.Status.FORBIDDEN).build();
            }
            if (clearIndex != null && clearIndex) indexingService.removeIndex();
            indexingService.indexAllDocuments();
            return Response.ok().build();
        } catch (Exception e) {
            LOG.error("Error occurred while deleting document index", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } finally {
            LOG.trace("Finished updateAllDocumentsIndex()");
        }
    }

    @ApiOperation(value = "Clear search index. The operation requires admin privileges")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully performed SOLR index clear"),
            @ApiResponse(code = 403, message = "The authorized subject is not an admin"),
            @ApiResponse(code = 500, message = "Internal Server Error performing SOLR index clear")
    })
    @DELETE
    @Path("searchIndex")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteDocumentsIndex(@Context ContainerRequestContext containerRequestContext) {
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

    @ApiOperation(value = "Delete document from index. The operation requires admin privileges")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully performed SOLR index clear"),
            @ApiResponse(code = 403, message = "The authorized subject is not an admin"),
            @ApiResponse(code = 500, message = "Internal Server Error performing SOLR index clear")
    })
    @DELETE
    @Path("searchIndex/{docId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteDocumentFromIndex(@PathParam("docId") Long docId, @Context ContainerRequestContext containerRequestContext) {
        LOG.trace("Start deleteDocumentsIndex()");
        try {
            User authorizedSubject = JacsSecurityContextHelper.getAuthorizedUser(containerRequestContext);
            if (authorizedSubject == null) {
                LOG.warn("Unauthorized attempt to delete document {} from index", docId);
                return Response.status(Response.Status.FORBIDDEN).build();
            }
            if (!authorizedSubject.hasGroupWrite(Group.ADMIN_KEY)) {
                LOG.warn("Non-admin user {} attempted to remove document {} from index", docId, authorizedSubject.getName());
                return Response.status(Response.Status.FORBIDDEN).build();
            }
            indexingService.removeFromIndexById(docId);
            return Response.noContent().build();
        } catch (Exception e) {
            LOG.error("Error occurred while deleting document index", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } finally {
            LOG.trace("Finished deleteDocumentsIndex()");
        }
    }

}
