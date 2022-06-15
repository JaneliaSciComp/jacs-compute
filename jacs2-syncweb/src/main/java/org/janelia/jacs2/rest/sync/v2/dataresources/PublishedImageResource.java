package org.janelia.jacs2.rest.sync.v2.dataresources;

import io.swagger.annotations.*;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.domain.dao.PublishedImageDao;
import org.janelia.model.domain.sample.PublishedImage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

@SwaggerDefinition(
        securityDefinition = @SecurityDefinition(
                apiKeyAuthDefinitions = {
                        @ApiKeyAuthDefinition(key = "user", name = "username", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER),
                        @ApiKeyAuthDefinition(key = "runAs", name = "runasuser", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER)
                }
        )
)
@Api(
        value = "Fly Light Published Image URLs",
        authorizations = {
                @Authorization("user"),
                @Authorization("runAs")
        }
)
@RequireAuthentication
@ApplicationScoped
@Path("/publishedImage")
public class PublishedImageResource {
    private static final Logger LOG = LoggerFactory.getLogger(PublishedImageResource.class);

    @Inject
    private PublishedImageDao publishedImageDao;

    @ApiOperation(value="Gets a Fly Light published image with S3 URLs")
    @ApiResponses(value={
            @ApiResponse(code=200,
                         message="Successfully got published image",
                         response=PublishedImage.class),
            @ApiResponse(code=400, message="No published 3D image found"),
            @ApiResponse(code=500, message="Internal Server Error getting published image")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/image/{alignmentSpace}/{objective}/{slideCode}")
    public Response getImage(@ApiParam @PathParam("alignmentSpace") String alignmentSpace,
                             @ApiParam @PathParam("slideCode") String slideCode,
                             @ApiParam @PathParam("objective") String objective) {
        LOG.trace("Start getImage({}, {}, {})", alignmentSpace,slideCode, objective);
        try {
            // minor note: the argument order is different in the DAO than in the REST API, because
            //  I was (still am?) uncertain as to what's best and have been changing my mind
            PublishedImage image = publishedImageDao.getImage(alignmentSpace, slideCode, objective);
            if (image == null) {
                LOG.warn("Could not find image for {}, {}, {}", alignmentSpace, objective, slideCode);
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorResponse("No image found for "
                                + alignmentSpace + ", " + slideCode + ", " + objective))
                        .build();
            }
            return Response
                    .ok(image)
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred getting published image for {}, {}, {}", alignmentSpace, slideCode, objective, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error retrieving published image for "
                            + alignmentSpace + ", " + slideCode+ ", " + objective))
                    .build();
        } finally {
            LOG.trace("Finished getImage({}, {}, {})", alignmentSpace, slideCode, objective);
        }
    }

    @ApiOperation(value="Gets Fly Light published images with S3 URLs")
    @ApiResponses(value={
            @ApiResponse(code=200, message="Successfully got published images",
                    response=PublishedImage.class,
                    responseContainer="List"),
            @ApiResponse(code=400, message="No published 3D image found"),
            @ApiResponse(code=500, message="Internal Server Error getting published images")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/imageWithGen1Image/{alignmentSpace}/{slideCode}/{objective}")
    public Response getImageWithGen1Image(@ApiParam @PathParam("alignmentSpace") String alignmentSpace,
                                          @ApiParam @PathParam("slideCode") String slideCode,
                                          @ApiParam @PathParam("objective") String objective) {
        LOG.trace("Start getImageWithGen1Image({}, {}, {})", alignmentSpace, slideCode, objective);
        // this is a combination of the two other endpoints, basically; we will always (?) use
        //  them together, so save one http call
        try {
            // minor note: the argument order is different in the DAO than in the REST API, because
            //  I was (still am?) uncertain as to what's best and have been changing my mind
            PublishedImage image = publishedImageDao.getImage(alignmentSpace, slideCode, objective);
            if (image == null) {
                LOG.warn("Could not find primary image for {}, {}, {}", alignmentSpace, objective, slideCode);
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorResponse("No image found for "
                                + alignmentSpace + ", " + objective + ", " + slideCode))
                        .build();
            }
            // now we get the second image, which could be null (not all images have one of these)
            PublishedImage gen1ExpressionImage = publishedImageDao.getGen1Gal4LexAImage(image.getOriginalLine(), image.getArea());
            return Response
                    .ok(new GenericEntity<Map<String, PublishedImage>>(new LinkedHashMap<String, PublishedImage>() {{
                        put("VisuallyLosslessStack", image);
                        put("SignalMipExpression", gen1ExpressionImage);
                    }}){})
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred getting published images for {}, {}, {}", alignmentSpace, slideCode, objective, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error retrieving published images for "
                            + alignmentSpace + ", " + slideCode+ ", " + objective))
                    .build();
        } finally {
            LOG.trace("Finished getImageWithGen1Image({}, {}, {})", alignmentSpace, slideCode, objective);
        }
    }
}