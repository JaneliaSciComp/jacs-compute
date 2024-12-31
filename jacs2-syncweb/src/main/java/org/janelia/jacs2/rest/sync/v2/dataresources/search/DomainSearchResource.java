package org.janelia.jacs2.rest.sync.v2.dataresources.search;

import java.util.List;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.solr.common.SolrDocumentList;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.dataservice.search.DocumentIndexingService;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.search.DocumentSearchParams;
import org.janelia.model.access.domain.search.DocumentSearchResults;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(name = "DomainSearch", description = "Janelia Workstation Domain Data Search")
@RequireAuthentication
@ApplicationScoped
@Path("/data")
public class DomainSearchResource {
    private static final Logger LOG = LoggerFactory.getLogger(DomainSearchResource.class);

    @Inject
    private DocumentIndexingService documentIndexingService;
    @Inject
    private LegacyDomainDao legacyDomainDao;

    @Operation(summary = "Performs a document search using the given search parameters",
            description = "Refer to the API docs on SOLRQuery for an explanation of the serialized parameters in search parameters"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully performed the search"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error performing SOLR Search")
    })
    @POST
    @Path("/search")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public DocumentSearchResults searchSolrIndices(DocumentSearchParams searchParams) {
        LOG.trace("Start searchSolrIndices({})", searchParams);
        try {
            DocumentSearchResults results = documentIndexingService.searchIndex(searchParams);
            LOG.debug("Document search found {} results, returning {}", results.getNumFound(), results.getResults().size());
            return results;
        } catch (Exception e) {
            LOG.error("Error occurred executing search with {}", searchParams, e);
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            LOG.trace("Finished searchSolrIndices({})", searchParams);
        }
    }

    @Operation(description = "Performs a document search using the given search parametersto find domain objects")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully performed the search"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error performing SOLR Search")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/domainobjects/search")
    public DocumentSearchResults searchDomainObjects(DocumentSearchParams searchParams) {
        LOG.trace("Start searchDomainObjects({})", searchParams);
        try {
            DocumentSearchResults searchResults = documentIndexingService.searchIndex(searchParams);
            LOG.debug("Solr search found {} results, returning {}", searchResults.getNumFound(), searchResults.getResults().size());
            SolrDocumentList docs = searchResults.getResults();
            List<DomainObject> details = legacyDomainDao.getDomainObjects(
                    searchParams.getSubjectKey(),
                    docs.stream().map(d -> Reference.createFor(d.get("type_label") + "#" + d.get("id"))).collect(Collectors.toList()));
            return new DocumentSearchResults(details, searchResults.getFacetValues(), docs.getNumFound());
        } catch (Exception e) {
            LOG.error("Error occurred executing search with {}", searchParams, e);
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            LOG.trace("Finished searchDomainObjects({})", searchParams);
        }
    }

}
