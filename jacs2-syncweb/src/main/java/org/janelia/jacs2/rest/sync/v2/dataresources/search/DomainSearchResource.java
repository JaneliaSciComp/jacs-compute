package org.janelia.jacs2.rest.sync.v2.dataresources.search;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Api(value = "Janelia Workstation Domain Data")
@RequireAuthentication
@Path("/data")
public class DomainSearchResource {
    private static final Logger LOG = LoggerFactory.getLogger(DomainSearchResource.class);

    @Inject private SolrConnector solr;

    @POST
    @Path("/search")
    @ApiOperation(value = "Performs a SOLRSearch using a SolrParams class",
            notes = "Refer to the API docs on SOLRQuery for an explanation of the serialized parameters in SolrParams"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully performed SOLR search", response=SolrJsonResults.class),
            @ApiResponse( code = 500, message = "Internal Server Error performing SOLR Search" )
    })
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public SolrJsonResults searchSolrIndices(@ApiParam SolrParams queryParams) {
        LOG.trace("Start searchSolrIndices({})", queryParams);
        try {
            SolrJsonResults results = getSearchResults(queryParams);
            LOG.debug("Solr search found {} results, returning {}",results.getNumFound(), results.getResults().size());
            return results;
        } catch (Exception e) {
            LOG.error("Error occurred executing search against SOLR", e);
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            LOG.trace("Finished searchSolrIndices({})", queryParams);
        }
    }

    private SolrJsonResults getSearchResults(SolrParams queryParams) {
        SolrQuery query = SolrQueryBuilder.deSerializeSolrQuery(queryParams);
        query.setFacetMinCount(1);
        query.setFacetLimit(500);
        QueryResponse response = solr.search(query);
        Map<String,List<FacetValue>> facetValues = new HashMap<>();
        if (response.getFacetFields()!=null) {
            for (final FacetField ff : response.getFacetFields()) {
                List<FacetValue> favetValues = new ArrayList<>();
                if (ff.getValues() != null) {
                    for (final FacetField.Count count : ff.getValues()) {
                        favetValues.add(new FacetValue(count.getName(), count.getCount()));
                    }
                }
                facetValues.put(ff.getName(), favetValues);
            }
        }

        return new SolrJsonResults(response.getResults(), facetValues, response.getResults().getNumFound());
    }

}
