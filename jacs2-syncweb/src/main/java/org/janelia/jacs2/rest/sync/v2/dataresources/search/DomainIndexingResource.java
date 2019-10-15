package org.janelia.jacs2.rest.sync.v2.dataresources.search;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableSet;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.dataservice.search.DocumentIndexingService;
import org.janelia.model.domain.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(value = "Janelia Workstation Domain Data")
@ApplicationScoped
@Path("/data")
public class DomainIndexingResource {
    private static final Logger LOG = LoggerFactory.getLogger(DomainIndexingResource.class);

    @Inject
    private DocumentIndexingService documentIndexingService;

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
            documentIndexingService.indexDocuments(domainObjectReferences);
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
                documentIndexingService.updateDocsAncestors(ImmutableSet.copyOf(descendantIds), ancestorId);
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
            documentIndexingService.removeDocuments(docIds);
            return Response.noContent().build();
        } catch (Exception e) {
            LOG.error("Error occurred while deleting documents {} index", docIds, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } finally {
            LOG.trace("Finished deleteDocumentsFromIndex({})", docIds);
        }
    }

}
