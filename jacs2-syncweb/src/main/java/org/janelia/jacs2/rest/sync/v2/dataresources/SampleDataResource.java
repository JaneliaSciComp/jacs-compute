package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

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
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.domain.sample.LSMImage;
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
        value = "Janelia Workstation Domain Data",
        authorizations = {
                @Authorization("user"),
                @Authorization("runAs")
        }
)
@RequireAuthentication
@Path("/data")
public class SampleDataResource {
    private static final Logger LOG = LoggerFactory.getLogger(SampleDataResource.class);

    @Inject
    private LegacyDomainDao legacyDomainDao;

    @ApiOperation(value = "Gets a list of LSMImage stacks for a sample",
            notes = "Uses the sample ID"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully got list of LSMImage stacks", response = LSMImage.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error getting list of LSMImage Stacks")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/sample/lsms")
    public Response getLsmsForSample(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                     @ApiParam @QueryParam("sampleId") final Long sampleId,
                                     @ApiParam @QueryParam("sageSynced") @DefaultValue("true") final String sageSynced) {
        LOG.trace("Start getLsmsForSample({}, {})", subjectKey, sampleId);
        try {
            List<LSMImage> sampleLSMs;
            if ("true".equals(sageSynced)) {
                sampleLSMs = legacyDomainDao.getActiveLsmsBySampleId(subjectKey, sampleId);
            } else if ("false".equals(sageSynced)) {
                sampleLSMs = legacyDomainDao.getInactiveLsmsBySampleId(subjectKey, sampleId);
            } else if ("both".equals(sageSynced)) {
                sampleLSMs = legacyDomainDao.getAllLsmsBySampleId(subjectKey, sampleId);
            } else {
                LOG.error("Invalid value for sageSynced flag - {} for retrieving sample {} LSMs for {}", sageSynced, sampleId, subjectKey);
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorResponse("Invalid sageSynced flag " + sageSynced + " only support {false, true, both}"))
                        .build();
            }
            return Response
                    .ok(new GenericEntity<List<LSMImage>>(sampleLSMs){})
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred getting sample {} LSMs for {}", sampleId, subjectKey, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error retrieving sample " + sampleId + " LSMs"))
                    .build();
        } finally {
            LOG.trace("Finished getLsmsForSample({}, {})", subjectKey, sampleId);
        }
    }

}
