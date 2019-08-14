package org.janelia.jacs2.rest.sync.v2.dataresources.search;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.solr.common.SolrDocumentList;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.dataservice.search.IndexingService;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.search.DocumentSearchParams;
import org.janelia.model.access.domain.search.DocumentSearchResults;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.stream.Collectors;

@Api(value = "Janelia Workstation Domain Data")
@RequireAuthentication
@ApplicationScoped
@Path("/data")
public class DomainSearchResource {
    private static final Logger LOG = LoggerFactory.getLogger(DomainSearchResource.class);

    @Inject
    private IndexingService indexingService;
    @Inject
    private LegacyDomainDao legacyDomainDao;

    @ApiOperation(value = "Performs a document search using the given search parameters",
            notes = "Refer to the API docs on SOLRQuery for an explanation of the serialized parameters in search parameters"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully performed the search", response = DocumentSearchResults.class),
            @ApiResponse(code = 500, message = "Internal Server Error performing SOLR Search")
    })
    @POST
    @Path("/search")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public DocumentSearchResults searchSolrIndices(@ApiParam DocumentSearchParams searchParams) {
        LOG.trace("Start searchSolrIndices({})", searchParams);
        try {
            DocumentSearchResults results = indexingService.searchIndex(searchParams);
            LOG.debug("Document search found {} results, returning {}", results.getNumFound(), results.getResults().size());
            return results;
        } catch (Exception e) {
            LOG.error("Error occurred executing search with {}", searchParams, e);
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            LOG.trace("Finished searchSolrIndices({})", searchParams);
        }
    }

    @ApiOperation(value = "Performs a document search using the given search parametersto find domain objects")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully performed the search", response = DocumentSearchResults.class),
            @ApiResponse(code = 500, message = "Internal Server Error performing SOLR Search")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/domainobjects/search")
    public DocumentSearchResults searchDomainObjects(@ApiParam DocumentSearchParams searchParams) {
        LOG.trace("Start searchDomainObjects({})", searchParams);
        try {
            DocumentSearchResults searchResults = indexingService.searchIndex(searchParams);
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
