package org.janelia.jacs2.rest.sync.v2.dataresources.search;

import java.util.List;
import java.util.concurrent.ExecutorService;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import com.google.common.collect.ImmutableSet;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.dataservice.search.DocumentIndexingService;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.cdi.WithCache;
import org.janelia.model.domain.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(name = "Indexing", description = "Janelia Workstation Domain Data Indexing")
@ApplicationScoped
@Path("/data")
public class DomainIndexingResource {
    private static final Logger LOG = LoggerFactory.getLogger(DomainIndexingResource.class);

    @Inject @AsyncIndex
    private ExecutorService asyncTaskExecutor;
    @Inject
    private Instance<DocumentIndexingService> documentIndexingServiceSource;
    @WithCache
    @Inject
    private Instance<DocumentIndexingService> documentIndexingServiceSourceWithCachedData;

    @Operation(summary = "Add document to index")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully added new document to SOLR index"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error while adding document to SOLR index")
    })
    @POST
    @Path("searchIndex")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public void indexDocuments(List<Reference> domainObjectReferences, @Suspended AsyncResponse asyncResponse) {
        asyncTaskExecutor.submit(() -> {
            LOG.trace("Start indexDocument({})", domainObjectReferences);
            try {
                DocumentIndexingService documentIndexingService;
                if (domainObjectReferences.size() > 10) {
                    documentIndexingService = documentIndexingServiceSourceWithCachedData.get();
                } else {
                    documentIndexingService = documentIndexingServiceSource.get();
                }
                int nDocs = documentIndexingService.indexDocuments(domainObjectReferences);
                asyncResponse.resume(nDocs);
            } catch (Throwable e) {
                LOG.error("Error occurred while adding {} to index", domainObjectReferences, e);
                asyncResponse.resume(e);
            } finally {
                LOG.trace("Finished indexDocument({})", domainObjectReferences);
            }
        });
    }

    @Operation(description = "Update the ancestor ids for the list of children specified in the body of the request")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully performed SOLR index update"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error performing SOLR index update")
    })
    @PUT
    @Path("searchIndex/{docId}/descendants")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateDocDescendants(@PathParam("docId") Long ancestorId, List<Long> descendantIds) {
        LOG.trace("Start addAncestorIdToAllDocs({}, {})", ancestorId, descendantIds);
        try {
            if (CollectionUtils.isNotEmpty(descendantIds)) {
                DocumentIndexingService documentIndexingService = documentIndexingServiceSourceWithCachedData.get();
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
    @Operation(summary = "Delete documents from index.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully removed document from index"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error performing document removal from index")
    })
    @POST
    @Path("searchIndex/docsToRemove")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteDocumentsFromIndex(List<Long> docIds) {
        LOG.trace("Start deleteDocumentsFromIndex({})", docIds);
        try {
            DocumentIndexingService documentIndexingService = documentIndexingServiceSource.get();
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
