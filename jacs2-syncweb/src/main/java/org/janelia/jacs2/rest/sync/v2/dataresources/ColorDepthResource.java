package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
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
import org.janelia.model.access.domain.dao.ColorDepthImageDao;
import org.janelia.model.domain.gui.cdmip.ColorDepthImage;
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
@ApplicationScoped
@Path("/data")
public class ColorDepthResource {
    private static final Logger LOG = LoggerFactory.getLogger(ColorDepthResource.class);

    @Inject
    private ColorDepthImageDao colorDepthImageDao;

    @ApiOperation(value = "Gets all color depth mips that match the given parameters")
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully fetched the list of color depth mips",  response = ColorDepthImage.class,
                    responseContainer = "List" ),
            @ApiResponse( code = 404, message = "Invalid id" ),
            @ApiResponse( code = 500, message = "Internal Server Error fetching the color depth mips" )
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("colorDepthMIPs/{id}")
    public Response getColorDepthMipById(@ApiParam @PathParam("id") Long id) {
        LOG.trace("Start getColorDepthMipById({})", id);
        try {
            ColorDepthImage cdm = colorDepthImageDao.findById(id);
            if (cdm == null) {
                return Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No color depth image found with id: " + id))
                        .build();
            } else {
                return Response
                        .ok(cdm)
                        .build();
            }
        } finally {
            LOG.trace("Finished getColorDepthMipById({})", id);
        }
    }

    @ApiOperation(value = "Counts all color depth mips that match the given parameters")
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully fetched the list of color depth mips",  response = ColorDepthImage.class,
                    responseContainer = "List" ),
            @ApiResponse( code = 500, message = "Internal Server Error fetching the color depth mips" )
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("colorDepthMIPsCount")
    public Response countColorDepthMipsByLibrary(@ApiParam @QueryParam("ownerKey") String ownerKey,
                                                 @ApiParam @QueryParam("libraryName") String libraryName,
                                                 @ApiParam @QueryParam("alignmentSpace") String alignmentSpace,
                                                 @ApiParam @QueryParam("name") List<String> names,
                                                 @ApiParam @QueryParam("filepath") List<String> filepaths) {
        LOG.trace("Start countColorDepthMipsByLibrary({}, {}, {}, {}, {})", ownerKey, libraryName, alignmentSpace, names, filepaths);
        try {
            long colorDepthMIPsCount = colorDepthImageDao.countColorDepthMIPs(ownerKey, libraryName, alignmentSpace, names, filepaths);
            return Response
                    .ok(colorDepthMIPsCount)
                    .build();
        } finally {
            LOG.trace("Finished countColorDepthMipsByLibrary({}, {}, {}, {}, {})", ownerKey, libraryName, alignmentSpace, names, filepaths);
        }
    }

    @ApiOperation(value = "Gets all color depth mips that match the given parameters")
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully fetched the list of color depth mips",  response = ColorDepthImage.class,
                    responseContainer = "List" ),
            @ApiResponse( code = 500, message = "Internal Server Error fetching the color depth mips" )
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("colorDepthMIPs")
    public Response getMatchingColorDepthMips(@ApiParam @QueryParam("ownerKey") String ownerKey,
                                              @ApiParam @QueryParam("libraryName") String libraryName,
                                              @ApiParam @QueryParam("alignmentSpace") String alignmentSpace,
                                              @ApiParam @QueryParam("name") List<String> names,
                                              @ApiParam @QueryParam("filepath") List<String> filepaths,
                                              @ApiParam @QueryParam("offset") Integer offset,
                                              @ApiParam @QueryParam("length") Integer length) {
        LOG.trace("Start getColorDepthMipsByLibrary({}, {}, {}, {}, {}, {}, {})", ownerKey, libraryName, alignmentSpace, names, filepaths, offset, length);
        try {
            Stream<ColorDepthImage> cdmStream = colorDepthImageDao.streamColorDepthMIPs(ownerKey, libraryName, alignmentSpace, names, filepaths, offset, length);
            return Response
                    .ok(new GenericEntity<List<ColorDepthImage>>(cdmStream.collect(Collectors.toList())){})
                    .build();
        } finally {
            LOG.trace("Finished getColorDepthMipsByLibrary({}, {}, {}, {}, {}, {}, {})", ownerKey, libraryName, alignmentSpace, names, filepaths, offset, length);
        }
    }

    @ApiOperation(value = "Update public URLs for the color depth image")
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully updated the specified color depth image", response = ColorDepthImage.class),
            @ApiResponse( code = 500, message = "Internal Server Error updating the color depth image" )
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("colorDepthMIPs/{id}/publicURLs")
    public Response updateColorDepthImagePublicURLs(@PathParam("id") Long id, ColorDepthImage colorDepthImage) {
        LOG.trace("Start updateColorDepthImagePublicURLs({}, {})", id, colorDepthImage);
        try {
            ColorDepthImage toUpdate = colorDepthImageDao.findById(id);
            if (toUpdate == null) {
                return Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No color depth image found with id: " + id))
                        .build();
            } else {
                toUpdate.setPublicImageUrl(colorDepthImage.getPublicImageUrl());
                toUpdate.setPublicThumbnailUrl(colorDepthImage.getPublicThumbnailUrl());
                colorDepthImageDao.updatePublicUrls(toUpdate);
                return Response
                        .ok(toUpdate)
                        .build();
            }
        } catch (Exception e) {
            LOG.error("Error occurred updating public urls for color depth image with id {}", id, e);
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error occurred updating public urls for color depth image with id " + id))
                    .build();
        } finally {
            LOG.trace("Finished updateColorDepthImagePublicURLs({}, {})", id, colorDepthImage);
        }
    }

}
