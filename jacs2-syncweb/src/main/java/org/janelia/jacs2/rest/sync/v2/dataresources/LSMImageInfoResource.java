package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
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
import org.janelia.model.access.domain.dao.LSMImageDao;
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
        value = "Janelia Workstation Line Release(s)",
        authorizations = {
                @Authorization("user"),
                @Authorization("runAs")
        }
)
@RequireAuthentication
@ApplicationScoped
@Path("/info")
public class LSMImageInfoResource {
    private static final Logger LOG = LoggerFactory.getLogger(LSMImageInfoResource.class);

    @Inject
    private LSMImageDao lsmImageDao;

    @ApiOperation(value = "Gets lsm stack by name")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully found and returned image", response = Response.class),
            @ApiResponse(code = 404, message = "LSM image not found"),
            @ApiResponse(code = 500, message = "Internal Server Error while looking up image")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("lsmstack/name/{name}")
    public Response getLSMStackByName(@ApiParam @PathParam("name") String imageName,
                                      @ApiParam @QueryParam("fuzzyMatch") Boolean fuzzyMatch,
                                      @ApiParam @QueryParam("includeNonSynced") Boolean includeNonSynced) {
        LOG.trace("Start getLSMStackByName({})", imageName);
        try {
            List<LSMImage> matchingImages;
            if (fuzzyMatch != null && fuzzyMatch) {
                matchingImages = lsmImageDao.findEntitiesByExactName(imageName);
            } else {
                matchingImages = lsmImageDao.findEntitiesWithMatchingName(imageName);
            }
            LSMImage lsmImage = matchingImages.stream()
                    .filter(lsm -> includeNonSynced != null && includeNonSynced || lsm.isLSMSageSynced())
                    .findFirst()
                    .orElse(null);
            if (lsmImage == null) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No image " + imageName + "found"))
                        .build();
            } else {
                return Response
                        .ok(lsmImage)
                        .build();
            }
        } catch (Exception e) {
            LOG.error("Error occurred getting image {}", imageName, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error retrieving image " + imageName))
                    .build();
        } finally {
            LOG.trace("Finished getLSMStackByName({})", imageName);
        }
    }

}
