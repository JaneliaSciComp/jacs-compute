package org.janelia.jacs2.rest.sync.v2.dataresources.search;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.janelia.jacs2.auth.JacsSecurityContextHelper;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.dataservice.search.SolrConnector;
import org.janelia.model.security.Group;
import org.janelia.model.security.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
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
    private static final int INDEXING_BATCH_SIZE = 200;

    @Inject
    private SolrConnector domainObjectIndexer;

    @ApiOperation(value = "Update the ancestor ids for the list of childrens specified in the body of the request")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully performed SOLR index update"),
            @ApiResponse(code = 500, message = "Internal Server Error performing SOLR index update")
    })
    @POST
    @Path("/searchIndex/{docId}/children")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateAncestorsForSolrDocs(@PathParam("docId") Long ancestorDocId, @ApiParam List<Long> documentIds) {
        LOG.trace("Start updateAncestorsForSolrDocs({}, {})", ancestorDocId, documentIds);
        try {
            domainObjectIndexer.updateAncestorsForSolrDocs(documentIds, ancestorDocId, INDEXING_BATCH_SIZE);
            return Response.ok().build();
        } catch (Exception e) {
            LOG.error("Error occurred while updating the ancestors for {} to {}", documentIds, ancestorDocId, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } finally {
            LOG.trace("Finished updateAncestorsForSolrDocs({}, {})", ancestorDocId, documentIds);
        }
    }

    @ApiOperation(value = "Clear search index. The operation requires admin privileges")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully performed SOLR index clear"),
            @ApiResponse(code = 403, message = "The authorized subject is not an admin"),
            @ApiResponse(code = 500, message = "Internal Server Error performing SOLR index clear")
    })
    @DELETE
    @Path("/searchIndex")
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
                LOG.info("Non-admin user {} attempted to remove search index", authorizedSubject.getName());
                return Response.status(Response.Status.FORBIDDEN).build();
            }
            domainObjectIndexer.clearIndex();
            return Response.ok().build();
        } catch (Exception e) {
            LOG.error("Error occurred while deleting document index", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } finally {
            LOG.trace("Finished deleteDocumentsIndex()");
        }
    }
}
