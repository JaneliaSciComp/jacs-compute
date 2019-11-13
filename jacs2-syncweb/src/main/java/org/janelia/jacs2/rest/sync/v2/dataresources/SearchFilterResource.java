package org.janelia.jacs2.rest.sync.v2.dataresources;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.gui.search.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SwaggerDefinition(
        securityDefinition = @SecurityDefinition(
                apiKeyAuthDefinitions = {
                        @ApiKeyAuthDefinition(key = "user", name = "username", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER),
                        @ApiKeyAuthDefinition(key = "runAs", name = "runasuser", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER)
                }
        )
)
@Api(
        value = "Janelia Data Service",
        authorizations = {
                @Authorization("user"),
                @Authorization("runAs")
        }
)
@RequireAuthentication
@ApplicationScoped
@Path("/data")
public class SearchFilterResource {

    private static final Logger LOG = LoggerFactory.getLogger(SearchFilterResource.class);

    @Inject
    private LegacyDomainDao legacyDomainDao;

    @ApiOperation(value = "Creates a Filter",
            notes = "uses the DomainObject parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully created a Filter", response = Filter.class),
            @ApiResponse( code = 500, message = "Internal Server Error creating a Filter" )
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/filter")
    public Filter createFilter(@ApiParam DomainQuery query) {
        LOG.trace("createFilter({})",query);
        try {
            return legacyDomainDao.save(query.getSubjectKey(), query.getDomainObjectAs(Filter.class));
        } catch (Exception e) {
            LOG.error("Error persisting search filter for {}", query, e);
            throw new WebApplicationException(e);
        } finally {
            LOG.trace("Finished createFilter({})", query);
        }
    }

    @ApiOperation(value = "Updates a Filter",
            notes = "uses the DomainObject parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully Updated a Filter", response = Filter.class),
            @ApiResponse( code = 500, message = "Internal Server Error Updating a Filter" )
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/filter")
    public Filter updateFilter(@ApiParam DomainQuery query) {
        LOG.debug("updateFilter({})",query);
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
