package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.ColorDepthImageDao;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.gui.cdmip.ColorDepthImage;
import org.janelia.model.domain.sample.Sample;
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

    static class CdmipWithSample {
        @JsonProperty
        private final String id;
        @JsonProperty
        private final ColorDepthImage cdmip;
        @JsonProperty
        private final Sample sample;

        CdmipWithSample(String id, ColorDepthImage cdmip, Sample sample) {
            this.id = id;
            this.cdmip = cdmip;
            this.sample = sample;
        }
    }

    @Inject
    private ColorDepthImageDao colorDepthImageDao;
    @Inject
    private LegacyDomainDao legacyDomainDao;

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
                                                 @ApiParam @QueryParam("alignmentSpace") String alignmentSpace,
                                                 @ApiParam @QueryParam("libraryName") List<String> libraryNames,
                                                 @ApiParam @QueryParam("name") List<String> names,
                                                 @ApiParam @QueryParam("filepath") List<String> filepaths) {
        LOG.trace("Start countColorDepthMipsByLibrary({}, {}, {}, {}, {})", ownerKey, alignmentSpace, libraryNames, names, filepaths);
        try {
            long colorDepthMIPsCount = colorDepthImageDao.countColorDepthMIPs(ownerKey, alignmentSpace,
                    extractMultiValueParams(libraryNames),
                    extractMultiValueParams(names),
                    extractMultiValueParams(filepaths)
            );
            return Response
                    .ok(colorDepthMIPsCount)
                    .build();
        } finally {
            LOG.trace("Finished countColorDepthMipsByLibrary({}, {}, {}, {}, {})", ownerKey, alignmentSpace, libraryNames, names, filepaths);
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
                                              @ApiParam @QueryParam("alignmentSpace") String alignmentSpace,
                                              @ApiParam @QueryParam("libraryName") List<String> libraryNames,
                                              @ApiParam @QueryParam("name") List<String> names,
                                              @ApiParam @QueryParam("filepath") List<String> filepaths,
                                              @ApiParam @QueryParam("offset") String offsetParam,
                                              @ApiParam @QueryParam("length") String lengthParam) {
        LOG.trace("Start getColorDepthMipsByLibrary({}, {}, {}, {}, {}, {}, {})", ownerKey, alignmentSpace, libraryNames, names, filepaths, offsetParam, lengthParam);
        try {
            int offset = parseIntegerParam("offset", offsetParam, 0);
            int length = parseIntegerParam("length", lengthParam, -1);
            Stream<ColorDepthImage> cdmStream = colorDepthImageDao.streamColorDepthMIPs(ownerKey, alignmentSpace,
                    extractMultiValueParams(libraryNames),
                    extractMultiValueParams(names),
                    extractMultiValueParams(filepaths),
                    offset,
                    length
            );
            return Response
                    .ok(new GenericEntity<List<ColorDepthImage>>(cdmStream.collect(Collectors.toList())){})
                    .build();
        } finally {
            LOG.trace("Finished getColorDepthMipsByLibrary({}, {}, {}, {}, {}, {}, {})", ownerKey, alignmentSpace, libraryNames, names, filepaths, offsetParam, lengthParam);
        }
    }

    @ApiOperation(value = "Gets all color depth mips that match the given parameters with the corresponding samples")
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully fetched the list of color depth mips",  response = CdmipWithSample.class,
                    responseContainer = "List" ),
            @ApiResponse( code = 500, message = "Internal Server Error fetching the color depth mips" )
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("colorDepthMIPsWithSamples")
    public Response getMatchingColorDepthMipsWithSample(@ApiParam @QueryParam("ownerKey") String ownerKey,
                                                        @ApiParam @QueryParam("alignmentSpace") String alignmentSpace,
                                                        @ApiParam @QueryParam("libraryName") List<String> libraryNames,
                                                        @ApiParam @QueryParam("name") List<String> names,
                                                        @ApiParam @QueryParam("filepath") List<String> filepaths,
                                                        @ApiParam @QueryParam("offset") String offsetParam,
                                                        @ApiParam @QueryParam("length") String lengthParam) {
        LOG.trace("Start getMatchingColorDepthMipsWithSample({}, {}, {}, {}, {}, {}, {})", ownerKey, alignmentSpace, libraryNames, names, filepaths, offsetParam, lengthParam);
        try {
            int offset = parseIntegerParam("offset", offsetParam, 0);
            int length = parseIntegerParam("length", lengthParam, -1);
            List<ColorDepthImage> cdmList = colorDepthImageDao.streamColorDepthMIPs(ownerKey, alignmentSpace,
                    extractMultiValueParams(libraryNames),
                    extractMultiValueParams(names),
                    extractMultiValueParams(filepaths),
                    offset,
                    length
            ).collect(Collectors.toList());

            List<Reference> sampleRefs =  cdmList.stream().map(cdmip -> cdmip.getSampleRef()).filter(sref -> sref != null).distinct().collect(Collectors.toList());

            Map<Reference, Sample> samples = legacyDomainDao.getDomainObjectsAs(sampleRefs, Sample.class).stream().collect(Collectors.toMap(s -> Reference.createFor(s), s -> s));

            return Response
                    .ok(new GenericEntity<List<CdmipWithSample>>(cdmList.stream().map(cdmip -> new CdmipWithSample(cdmip.getId().toString(), cdmip, samples.get(cdmip.getSampleRef()))).collect(Collectors.toList())){})
                    .build();
        } finally {
            LOG.trace("Finished getMatchingColorDepthMipsWithSample({}, {}, {}, {}, {}, {}, {})", ownerKey, alignmentSpace, libraryNames, names, filepaths, offsetParam, lengthParam);
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

    private Integer parseIntegerParam(String paramName, String paramValue, Integer defaultValue) {
        try {
            return StringUtils.isNotBlank(paramValue) ? Integer.parseInt(paramValue) : defaultValue;
        } catch (NumberFormatException e) {

            throw new IllegalArgumentException("Invalid numeric value " + paramName + "->" + paramValue, e);
        }
    }
}
