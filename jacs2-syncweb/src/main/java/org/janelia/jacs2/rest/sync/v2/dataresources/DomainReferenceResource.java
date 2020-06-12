package org.janelia.jacs2.rest.sync.v2.dataresources;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.model.access.domain.dao.ReferenceDomainObjectReadDao;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Web service for handling Node entities.
 */
@SwaggerDefinition(
        securityDefinition = @SecurityDefinition(
                apiKeyAuthDefinitions = {
                        @ApiKeyAuthDefinition(key = "user", name = "username", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER),
                        @ApiKeyAuthDefinition(key = "runAs", name = "runasuser", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER)
                }
        )
)
@Api(
        value = "Data Node Service",
        authorizations = {
                @Authorization("user"),
                @Authorization("runAs")
        }
)
@RequireAuthentication
@ApplicationScoped
@Path("/reference")
public class DomainReferenceResource {

    private static final Logger LOG = LoggerFactory.getLogger(DomainReferenceResource.class);

    @Inject
    private ReferenceDomainObjectReadDao referenceDomainObjectReadDao;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{classname}/{id}")
    public <T extends DomainObject> Response getDataNode(@PathParam("classname") String classname,
                                                         @PathParam("id") String id) {
        T domainData = referenceDomainObjectReadDao.findByReference(Reference.createFor(classname + "#" + id));
        if (domainData == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        }
        return Response
                .ok(domainData)
                .build();
    }

}
