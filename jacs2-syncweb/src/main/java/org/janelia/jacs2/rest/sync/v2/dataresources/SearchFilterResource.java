package org.janelia.jacs2.rest.sync.v2.dataresources;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.gui.search.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(name = "SearchFilter",
        description = "Janelia Data Service"
)
@RequireAuthentication
@ApplicationScoped
@Path("/data")
public class SearchFilterResource {

    private static final Logger LOG = LoggerFactory.getLogger(SearchFilterResource.class);

    @Inject
    private LegacyDomainDao legacyDomainDao;

    @Operation(summary = "Creates a Filter",
            description = "uses the DomainObject parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully created a Filter"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error creating a Filter")
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/filter")
    public Filter createFilter(@Parameter DomainQuery query) {
        LOG.trace("createFilter({})", query);
        try {
            return legacyDomainDao.save(query.getSubjectKey(), query.getDomainObjectAs(Filter.class));
        } catch (Exception e) {
            LOG.error("Error persisting search filter for {}", query, e);
            throw new WebApplicationException(e);
        } finally {
            LOG.trace("Finished createFilter({})", query);
        }
    }

    @Operation(summary = "Updates a Filter",
            description = "uses the DomainObject parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully Updated a Filter"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error Updating a Filter")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/filter")
    public Filter updateFilter(@Parameter DomainQuery query) {
        LOG.debug("updateFilter({})", query);
        try {
            return legacyDomainDao.save(query.getSubjectKey(), query.getDomainObjectAs(Filter.class));
        } catch (Exception e) {
            LOG.error("Error occurred updating search filter {}", query, e);
            throw new WebApplicationException(e);
        } finally {
            LOG.trace("Finished updateFilter({})", query);
        }
    }

}
