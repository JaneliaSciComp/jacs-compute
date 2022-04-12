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
import java.util.List;

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
            @ApiResponse(code=200, message="Successfully got published image",
                response=PublishedImage.class,
                responseContainer="List"),
            @ApiResponse(code=500, message="Internal Server Error getting published image")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/image/{alignmentSpace}/{objective}/{slideCode}")
    public Response getImage(@ApiParam @PathParam("alignmentSpace") String alignmentSpace,
                             @ApiParam @PathParam("objective") String objective,
                             @ApiParam @PathParam("slideCode") String slideCode) {
        LOG.trace("Start getImage({}, {}, {})", alignmentSpace, objective, slideCode);
        try {
            // minor note: the argument order is different in the DAO than in the REST API, because
            //  I was (still am?) uncertain as to what's best and have been changing my mind
            PublishedImage image = publishedImageDao.getImage(slideCode, alignmentSpace, objective);
            if (image == null) {
                LOG.warn("Could not find image for {}, {}, {}", alignmentSpace, objective, slideCode);
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorResponse("No image found for "
                                + alignmentSpace + ", " + objective + ", " + slideCode))
                        .build();
            }

            // returning a list even though it's a single image; I can imagine changing the API in
            //  the future to allow multiple images to be returned, so allow for that
            List<PublishedImage> publishedImages = new ArrayList<>();
            publishedImages.add(image);
            return Response
                    .ok(new GenericEntity<List<PublishedImage>>(publishedImages){})
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred getting published image for {}, {}, {}", alignmentSpace, objective, slideCode, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error retrieving published image for "
                            + alignmentSpace + ", " + objective+ ", " + slideCode))
                    .build();
        } finally {
            LOG.trace("Finished getImage({}, {}, {})", alignmentSpace, objective, slideCode);
        }
    }

    @ApiOperation(value="Gets a Fly Light published image with S3 URLs")
    @ApiResponses(value={
            @ApiResponse(code=200, message="Successfully got published image",
                    response=PublishedImage.class,
                    responseContainer="List"),
            @ApiResponse(code=500, message="Internal Server Error getting published image")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/imageForGen1Gal4LexA/{originalLine}/{area}")
    public Response getGen1Gal4LexA(@ApiParam @PathParam("originalLine") String originalLine,
                                    @ApiParam @PathParam("area") String area) {
        LOG.trace("Start getGen1Gal4LexA({}, {})", originalLine, area);
        try {
            PublishedImage image = publishedImageDao.getGen1Gal4LexAImage(originalLine, area);
            if (image == null) {
                LOG.warn("Could not find image for {}, {}", originalLine, area);
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorResponse("No image found for "
                                + originalLine + ", " + area))
                        .build();
            }

            // returning a list even though it's a single image; I can imagine changing the API in
            //  the future to allow multiple images to be returned, so allow for that
            List<PublishedImage> publishedImages = new ArrayList<>();
            publishedImages.add(image);
            return Response
                    .ok(new GenericEntity<List<PublishedImage>>(publishedImages){})
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred getting published image for {}, {}", originalLine, area, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error retrieving published image for "
                            + originalLine + ", " + area))
                    .build();
        } finally {
            LOG.trace("Finished getGen1Gal4LexA({}, {})", originalLine, area);
        }

    }

    @ApiOperation(value="Gets Fly Light published images with S3 URLs")
    @ApiResponses(value={
            @ApiResponse(code=200, message="Successfully got published images",
                    response=PublishedImage.class,
                    responseContainer="List"),
            @ApiResponse(code=500, message="Internal Server Error getting published images")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/imageWithGen1Image/{alignmentSpace}/{objective}/{slideCode}")
    public Response getImageWithGen1Image(@ApiParam @PathParam("alignmentSpace") String alignmentSpace,
                             @ApiParam @PathParam("objective") String objective,
                             @ApiParam @PathParam("slideCode") String slideCode) {
        LOG.trace("Start getImageWithGen1Image({}, {}, {})", alignmentSpace, objective, slideCode);
        // this is a combination of the two other endpoints, basically; we will always (?) use
        //  them together, so save one http call
        try {
            // minor note: the argument order is different in the DAO than in the REST API, because
            //  I was (still am?) uncertain as to what's best and have been changing my mind
            PublishedImage image = publishedImageDao.getImage(slideCode, alignmentSpace, objective);
            if (image == null) {
                LOG.warn("Could not find primary image for {}, {}, {}", alignmentSpace, objective, slideCode);
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorResponse("No image found for "
                                + alignmentSpace + ", " + objective + ", " + slideCode))
                        .build();
            }

            // returning a list even though it's a single image; I can imagine changing the API in
            //  the future to allow multiple images to be returned, so allow for that
            List<PublishedImage> publishedImages = new ArrayList<>();
            publishedImages.add(image);

            // now we get the second image, which could be null (not all images have one of these)
            PublishedImage image2 = publishedImageDao.getGen1Gal4LexAImage(image.getOriginalLine(), image.getArea());
            publishedImages.add(image2);

            return Response
                    .ok(new GenericEntity<List<PublishedImage>>(publishedImages){})
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred getting published images for {}, {}, {}", alignmentSpace, objective, slideCode, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error retrieving published images for "
                            + alignmentSpace + ", " + objective+ ", " + slideCode))
                    .build();
        } finally {
            LOG.trace("Finished getImageWithGen1Image({}, {}, {})", alignmentSpace, objective, slideCode);
        }
    }
}