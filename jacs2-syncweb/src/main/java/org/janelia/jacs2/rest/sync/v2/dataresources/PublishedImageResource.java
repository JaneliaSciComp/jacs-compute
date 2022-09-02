package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Splitter;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.domain.dao.PublishedImageDao;
import org.janelia.model.domain.sample.PublishedImage;
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

    public static class PublishedImageWithGal4Expression {
        @JsonProperty
        final PublishedImage visuallyLosslessStack;
        @JsonProperty
        final PublishedImage gal4Expression;

        PublishedImageWithGal4Expression(PublishedImage visuallyLosslessStack, PublishedImage gal4Expression) {
            this.visuallyLosslessStack = visuallyLosslessStack;
            this.gal4Expression = gal4Expression;
        }
    }

    @Inject
    private PublishedImageDao publishedImageDao;

    @ApiOperation(value="Gets a Fly Light published image with S3 URLs")
    @ApiResponses(value={
            @ApiResponse(code=200, message="Successfully got published image",
                         response=PublishedImage.class),
            @ApiResponse(code=400, message="No published 3D image found"),
            @ApiResponse(code=500, message="Internal Server Error getting published image")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/image/{alignmentSpace}/{objective}")
    public Response getImage(@ApiParam @PathParam("alignmentSpace") String alignmentSpace,
                             @ApiParam @PathParam("objective") String objective,
                             @ApiParam @QueryParam("slideCode") List<String> slideCodesParam) {
        LOG.trace("Start getImage({}, {}, {})", alignmentSpace, objective, slideCodesParam);
        try {
            List<String> slideCodes = extractMultiValueParams(slideCodesParam);
            List<PublishedImage> images = publishedImageDao.getImages(alignmentSpace, slideCodes, objective);
            return Response
                    .ok(images)
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred getting published image for {}, {}, {}", alignmentSpace, objective, slideCodesParam, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error retrieving published image for "
                            + alignmentSpace + ", " + objective+ ", " + slideCodesParam))
                    .build();
        } finally {
            LOG.trace("Finished getImage({}, {}, {})", alignmentSpace, objective, slideCodesParam);
        }
    }

    @ApiOperation(value="Gets Fly Light published images with S3 URLs")
    @ApiResponses(value={
            @ApiResponse(code=200, message="Successfully got published images",
                    response=PublishedImageWithGal4Expression.class,
                    responseContainer="List"),
            @ApiResponse(code=400, message="No published 3D image found"),
            @ApiResponse(code=500, message="Internal Server Error getting published images")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/imageWithGen1Image/{alignmentSpace}/{objective}")
    public Response getImageWithGen1Image(@ApiParam @PathParam("alignmentSpace") String alignmentSpace,
                                          @ApiParam @PathParam("objective") String objective,
                                          @ApiParam @QueryParam("slideCode") List<String> slideCodesParam) {
        LOG.trace("Start getImageWithGen1Image({}, {}, {})", alignmentSpace, objective, slideCodesParam);
        // this is a combination of the two other endpoints, basically; we will always (?) use
        //  them together, so save one http call
        try {
            List<String> slideCodes = extractMultiValueParams(slideCodesParam);
            List<PublishedImage> images = publishedImageDao.getImages(alignmentSpace, slideCodes, objective);
            Map<String, Set<String>> originalLinesByArea = images.stream().collect(Collectors.groupingBy(
                    PublishedImage::getArea,
                    Collectors.collectingAndThen(Collectors.toSet(), publishedImages -> publishedImages.stream().map(PublishedImage::getOriginalLine).collect(Collectors.toSet())
                    )
            ));
            // now we get the second image, which could be null (not all images have one of these)
            Map<Pair<String, String>, PublishedImage> gal4ExpressionsByAreaAndLine = originalLinesByArea.entrySet().stream()
                    .flatMap(e -> publishedImageDao.getGen1Gal4LexAImages(e.getKey(), e.getValue()).stream())
                    .collect(Collectors.toMap(
                            i -> ImmutablePair.of(i.getArea(), i.getOriginalLine()),
                            i -> i
                    ));

            List<PublishedImageWithGal4Expression> results = images.stream()
                    .map(i -> new PublishedImageWithGal4Expression(
                            i,
                            gal4ExpressionsByAreaAndLine.get(ImmutablePair.of(i.getArea(), i.getOriginalLine()))))
                    .collect(Collectors.toList());
            return Response
                    .ok(new GenericEntity<List<PublishedImageWithGal4Expression>>(results){})
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred getting published images for {}, {}, {}", alignmentSpace, objective, slideCodesParam, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error retrieving published images for "
                            + alignmentSpace + ", " + objective+ ", " + slideCodesParam))
                    .build();
        } finally {
            LOG.trace("Finished getImageWithGen1Image({}, {}, {})", alignmentSpace, objective, slideCodesParam);
        }
    }

    private List<String> extractMultiValueParams(List<String> params) {
        if (CollectionUtils.isEmpty(params)) {
            return Collections.emptyList();
        } else {
            return params.stream()
                    .filter(StringUtils::isNotBlank)
                    .flatMap(param -> Splitter.on(',').trimResults().omitEmptyStrings().splitToList(param).stream())
                    .collect(Collectors.toList())
                    ;
        }
    }

}