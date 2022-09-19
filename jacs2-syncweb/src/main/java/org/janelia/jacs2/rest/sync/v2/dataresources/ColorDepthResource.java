package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
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
import org.janelia.model.access.domain.dao.ColorDepthImageDao;
import org.janelia.model.access.domain.dao.ColorDepthImageQuery;
import org.janelia.model.access.domain.dao.LineReleaseDao;
import org.janelia.model.access.domain.dao.ReferenceDomainObjectReadDao;
import org.janelia.model.access.domain.dao.SampleDao;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.flyem.EMBody;
import org.janelia.model.domain.flyem.EMDataSet;
import org.janelia.model.domain.gui.cdmip.ColorDepthImage;
import org.janelia.model.domain.gui.cdmip.ColorDepthImageWithNeuronsBuilder;
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

    @Inject
    private ColorDepthImageDao colorDepthImageDao;
    @Inject
    private SampleDao sampleDao;
    @Inject
    private ReferenceDomainObjectReadDao referenceDao;
    @Inject
    private LineReleaseDao lineReleaseDao;

    @ApiOperation(value = "Gets all color depth mips that match the given parameters")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched the list of color depth mips", response = ColorDepthImage.class,
                    responseContainer = "List"),
            @ApiResponse(code = 404, message = "Invalid id"),
            @ApiResponse(code = 500, message = "Internal Server Error fetching the color depth mips")
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
            @ApiResponse(code = 200, message = "Successfully fetched the list of color depth mips", response = ColorDepthImage.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error fetching the color depth mips")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    @Path("colorDepthMIPsCount")
    public Response countColorDepthMipsByLibrary(@ApiParam @QueryParam("ownerKey") String ownerKey,
                                                 @ApiParam @QueryParam("alignmentSpace") String alignmentSpace,
                                                 @ApiParam @QueryParam("libraryName") List<String> libraryNames,
                                                 @ApiParam @QueryParam("id") List<String> ids,
                                                 @ApiParam @QueryParam("name") List<String> names,
                                                 @ApiParam @QueryParam("filepath") List<String> filepaths,
                                                 @ApiParam @QueryParam("dataset") List<String> datasets,
                                                 @ApiParam @QueryParam("release") List<String> releases) {
        LOG.trace("Start countColorDepthMipsByLibrary({}, {}, {}, {}, {}, {})", ownerKey, alignmentSpace, libraryNames, names, filepaths, datasets);
        try {
            List<String> sampleRefs = retrieveSampleRefs(extractMultiValueParams(datasets), extractMultiValueParams(releases)).stream()
                    .map(Reference::toString)
                    .collect(Collectors.toList());
            List<Long> mipIds = extractMultiValueParams(ids).stream().map(Long::valueOf).collect(Collectors.toList());
            long colorDepthMIPsCount = colorDepthImageDao.countColorDepthMIPs(
                    new ColorDepthImageQuery()
                            .withOwner(ownerKey)
                            .withAlignmentSpace(alignmentSpace)
                            .withLibraryIdentifiers(extractMultiValueParams(libraryNames))
                            .withIds(mipIds)
                            .withExactNames(extractMultiValueParams(names))
                            .withExactFilepaths(extractMultiValueParams(filepaths))
                            .withSampleRefs(sampleRefs)
            );
            return Response
                    .ok(colorDepthMIPsCount)
                    .build();
        } finally {
            LOG.trace("Finished countColorDepthMipsByLibrary({}, {}, {}, {}, {}, {})", ownerKey, alignmentSpace, libraryNames, names, filepaths, datasets);
        }
    }

    @ApiOperation(value = "Counts all color depth mips per alignment space for the given library")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched the color depth mips count", response = Map.class,
                    responseContainer = "Map"),
            @ApiResponse(code = 500, message = "Internal Server Error counting the color depth mips")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    @Path("colorDepthMIPsCountPerAlignmentSpace")
    public Response countColorDepthMipsPerAlignmentSpaceByLibrary(@ApiParam @QueryParam("libraryName") String libraryName) {
        LOG.trace("Start countColorDepthMipsPerAlignmentSpaceByLibrary({})", libraryName);
        try {
            return Response
                    .ok(colorDepthImageDao.countColorDepthMIPsByAlignmentSpaceForLibrary(libraryName))
                    .build();
        } finally {
            LOG.trace("Finished countColorDepthMipsPerAlignmentSpaceByLibrary({})", libraryName);
        }
    }

    @ApiOperation(value = "Gets all color depth mips that match the given parameters")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched the list of color depth mips", response = ColorDepthImage.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error fetching the color depth mips")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("colorDepthMIPs")
    public Response getMatchingColorDepthMips(@ApiParam @QueryParam("ownerKey") String ownerKey,
                                              @ApiParam @QueryParam("alignmentSpace") String alignmentSpace,
                                              @ApiParam @QueryParam("libraryName") List<String> libraryNames,
                                              @ApiParam @QueryParam("id") List<String> ids,
                                              @ApiParam @QueryParam("name") List<String> names,
                                              @ApiParam @QueryParam("filepath") List<String> filepaths,
                                              @ApiParam @QueryParam("dataset") List<String> datasets,
                                              @ApiParam @QueryParam("release") List<String> releases,
                                              @ApiParam @QueryParam("offset") String offsetParam,
                                              @ApiParam @QueryParam("length") String lengthParam) {
        LOG.trace("Start getColorDepthMipsByLibrary({}, {}, {}, {}, {}, {}, {}, {})", ownerKey, alignmentSpace, libraryNames, names, filepaths, datasets, offsetParam, lengthParam);
        try {
            int offset = parseIntegerParam("offset", offsetParam, 0);
            int length = parseIntegerParam("length", lengthParam, -1);
            List<String> sampleRefs = retrieveSampleRefs(extractMultiValueParams(datasets), extractMultiValueParams(releases)).stream()
                    .map(Reference::toString)
                    .collect(Collectors.toList());
            List<Long> mipIds = extractMultiValueParams(ids).stream().map(Long::valueOf).collect(Collectors.toList());
            Stream<ColorDepthImage> cdmStream = colorDepthImageDao.streamColorDepthMIPs(
                    new ColorDepthImageQuery()
                            .withOwner(ownerKey)
                            .withAlignmentSpace(alignmentSpace)
                            .withLibraryIdentifiers(extractMultiValueParams(libraryNames))
                            .withIds(mipIds)
                            .withExactNames(extractMultiValueParams(names))
                            .withExactFilepaths(extractMultiValueParams(filepaths))
                            .withSampleRefs(sampleRefs)
                            .withOffset(offset)
                            .withLength(length)
            );
            return Response
                    .ok(new GenericEntity<List<ColorDepthImage>>(cdmStream.collect(Collectors.toList())) {
                    })
                    .build();
        } finally {
            LOG.trace("Finished getColorDepthMipsByLibrary({}, {}, {}, {}, {}, {}, {}, {})", ownerKey, alignmentSpace, libraryNames, names, filepaths, datasets, offsetParam, lengthParam);
        }
    }

    @ApiOperation(value = "Gets all color depth mips that match the given parameters with the corresponding samples")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched the list of color depth mips", response = ColorDepthImage.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error fetching the color depth mips")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("colorDepthMIPsWithSamples")
    public Response getMatchingColorDepthMipsWithSample(@ApiParam @QueryParam("ownerKey") String ownerKey,
                                                        @ApiParam @QueryParam("alignmentSpace") String alignmentSpace,
                                                        @ApiParam @QueryParam("libraryName") List<String> libraryNames,
                                                        @ApiParam @QueryParam("id") List<String> ids,
                                                        @ApiParam @QueryParam("name") List<String> names,
                                                        @ApiParam @QueryParam("filepath") List<String> filepaths,
                                                        @ApiParam @QueryParam("dataset") List<String> datasets,
                                                        @ApiParam @QueryParam("release") List<String> releases,
                                                        @ApiParam @QueryParam("offset") String offsetParam,
                                                        @ApiParam @QueryParam("length") String lengthParam) {
        LOG.trace("Start getMatchingColorDepthMipsWithSample({}, {}, {}, {}, {}, {}, {}, {})", ownerKey, alignmentSpace, libraryNames, ids, names, filepaths, offsetParam, lengthParam);
        long start = System.currentTimeMillis();
        try {
            int offset = parseIntegerParam("offset", offsetParam, 0);
            int length = parseIntegerParam("length", lengthParam, -1);
            List<Reference> sampleRefs = retrieveSampleRefs(extractMultiValueParams(datasets), extractMultiValueParams(releases));
            List<Long> mipIds = extractMultiValueParams(ids).stream().map(Long::valueOf).collect(Collectors.toList());
            LOG.info("Retrieved {} sample refs after {}ms", sampleRefs.size(), System.currentTimeMillis() - start);
            List<ColorDepthImage> cdmList = colorDepthImageDao.streamColorDepthMIPs(
                    new ColorDepthImageQuery()
                            .withOwner(ownerKey)
                            .withAlignmentSpace(alignmentSpace)
                            .withLibraryIdentifiers(extractMultiValueParams(libraryNames))
                            .withIds(mipIds)
                            .withExactNames(extractMultiValueParams(names))
                            .withExactFilepaths(extractMultiValueParams(filepaths))
                            .withSampleRefs(sampleRefs.stream().map(Reference::toString).collect(Collectors.toSet()))
                            .withOffset(offset)
                            .withLength(length)
            ).collect(Collectors.toList());
            LOG.info("Retrieved {} CDMs after {}ms", cdmList.size(), System.currentTimeMillis() - start);
            Map<Reference, Sample> indexedCDMIPSamples = retrieveDomainObjectsByRefs(
                    cdmList.stream()
                            .map(ColorDepthImage::getSampleRef)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toSet()),
                    s -> s.setObjectiveSamples(Collections.emptyList())); // clean up objective samples to reduce the bandwidth

            Map<Reference, EMBody> indexedCDMIPBodies = retrieveDomainObjectsByRefs(
                    cdmList.stream()
                            .map(ColorDepthImage::getEmBodyRef)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toSet()),
                    emb -> {});
            Map<Reference, EMDataSet> indexedEMDataSets = retrieveDomainObjectsByRefs(
                    indexedCDMIPBodies.values().stream()
                            .map(EMBody::getDataSetRef)
                            .collect(Collectors.toSet()),
                    emds -> {});
            return Response
                    .ok(new GenericEntity<List<ColorDepthImage>>(
                            updateCDMIPSample(
                                    cdmList,
                                    indexedCDMIPSamples,
                                    indexedCDMIPBodies,
                                    indexedEMDataSets)) {
                    })
                    .build()
                    ;
        } finally {
            LOG.trace("Finished getMatchingColorDepthMipsWithSample({}, {}, {}, {}, {}, {}, {}, {})", ownerKey, alignmentSpace, libraryNames, ids, names, filepaths, offsetParam, lengthParam);
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends DomainObject> Map<Reference, T> retrieveDomainObjectsByRefs(Set<Reference> refs,
                                                                                   Consumer<T> domainObjectConsumer) {
        long start = System.currentTimeMillis();
        try {
            if (CollectionUtils.isEmpty(refs)) {
                return Collections.emptyMap();
            } else {
                return referenceDao.findByReferences(refs).stream()
                        .map(d -> (T) d)
                        .peek(domainObjectConsumer)
                        .collect(Collectors.toMap(Reference::createFor, Function.identity()))
                        ;
            }
        } finally {
            long end = System.currentTimeMillis();
            LOG.debug("Retrieve data from refs in {}ms", end - start);
        }
    }

    private List<ColorDepthImage> updateCDMIPSample(List<ColorDepthImage> cdmList,
                                                    Map<Reference, Sample> samplesIndexedByRef,
                                                    Map<Reference, EMBody> emBodiesIndexedByRef,
                                                    Map<Reference, EMDataSet> emDataSetsIndexedByRef) {
        long start = System.currentTimeMillis();
        try {
            // fill in dataset info
            emBodiesIndexedByRef.forEach((ref, emBody) -> emBody.setEmDataSet(emDataSetsIndexedByRef.get(emBody.getDataSetRef())));
            return cdmList.stream()
                    .map(cdmip -> new ColorDepthImageWithNeuronsBuilder(cdmip)
                            .withLMSample(samplesIndexedByRef.get(cdmip.getSampleRef()))
                            .withEMBody(emBodiesIndexedByRef.get(cdmip.getEmBodyRef()))
                            .build())
                    .collect(Collectors.toList())
                    ;
        } finally {
            long end = System.currentTimeMillis();
            LOG.debug("Update CDMIP with sample ref in {}ms", end - start);
        }
    }

    @ApiOperation(value = "Update public URLs for the color depth image")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated the specified color depth image", response = ColorDepthImage.class),
            @ApiResponse(code = 500, message = "Internal Server Error updating the color depth image")
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

    private List<Reference> retrieveSampleRefs(List<String> datasets, List<String> releases) {
        if (CollectionUtils.isEmpty(datasets) && CollectionUtils.isEmpty(releases)) {
            return Collections.emptyList();
        } else if (CollectionUtils.isEmpty(datasets)) {
            return lineReleaseDao.findReleasesByName(releases).stream()
                    .flatMap(r -> r.getChildren().stream())
                    .collect(Collectors.toList());
        } else if (CollectionUtils.isEmpty(datasets)) {
            return sampleDao.findMatchingSample(null, datasets, null, null, 0, -1).stream()
                    .map(Reference::createFor)
                    .collect(Collectors.toList());
        } else {
            List<Reference> sampleRefsForDatasets = sampleDao.findMatchingSample(null, datasets, null, null, 0, -1).stream()
                    .map(Reference::createFor)
                    .collect(Collectors.toList());
            List<Reference> sampleRefsForReleases = lineReleaseDao.findReleasesByName(releases).stream()
                    .flatMap(r -> r.getChildren().stream())
                    .collect(Collectors.toList());
            return new ArrayList<>(Sets.intersection(
                    ImmutableSet.copyOf(sampleRefsForDatasets),
                    ImmutableSet.copyOf(sampleRefsForReleases)));
        }
    }
}
