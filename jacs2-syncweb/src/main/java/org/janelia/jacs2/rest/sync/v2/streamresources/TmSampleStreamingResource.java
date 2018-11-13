package org.janelia.jacs2.rest.sync.v2.streamresources;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.domain.dao.TmSampleDao;
import org.janelia.model.cdi.WithCache;
import org.janelia.model.domain.tiledMicroscope.TmSample;
import org.janelia.model.rendering.CoordinateAxis;
import org.janelia.model.rendering.RenderedVolumeLoader;
import org.janelia.model.rendering.RenderingType;
import org.janelia.model.rendering.TileKey;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

@ApplicationScoped
@Produces("application/json")
@Path("/mouselight")
public class TmSampleStreamingResource {

    @Inject
    private TmSampleDao tmSampleDao;
    @WithCache
    @Inject
    private RenderedVolumeLoader renderedVolumeLoader;
    @Inject
    private Logger logger;

    @ApiOperation(value = "Get sample rendering info", notes = "Retrieve volume rendering info for the specified sample")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 404, message = "Sample not found or no rendering"),
            @ApiResponse(code = 500, message = "Error occurred")})
    @GET
    @Path("samples/{sampleId}/volume_info")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getSampleVolumeInfo(@PathParam("sampleId") Long sampleId) {
        TmSample tmSample = tmSampleDao.findById(sampleId);
        if (tmSample == null) {
            logger.warn("No sample found for {}", sampleId);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No sample found for " + sampleId))
                    .build();
        } else if (StringUtils.isBlank(tmSample.getFilepath())) {
            logger.warn("Sample {} found but it has not rendering path", tmSample);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No rendering path set for " + sampleId))
                    .build();
        }
        return renderedVolumeLoader.loadVolume(Paths.get(tmSample.getFilepath()))
                .map(rv -> Response.ok(rv).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("Error getting rendering info for " + sampleId))
                        .build())
                ;
    }

    @ApiOperation(value = "Get sample rendering info", notes = "Retrieve volume rendering info for the specified base folder")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 404, message = "Sample not found or no rendering"),
            @ApiResponse(code = 500, message = "Error occurred")})
    @GET
    @Path("volume_info/{baseFolder:.*}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getVolumeInfoFromBaseFolder(@PathParam("baseFolder") String baseFolderParam) {
        if (StringUtils.isBlank(baseFolderParam)) {
            logger.warn("No base folder has been specified: {}", baseFolderParam);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse("No base path has been specified"))
                    .build();
        }
        String baseFolderName = StringUtils.prependIfMissing(baseFolderParam, "/");
        return renderedVolumeLoader.loadVolume(Paths.get(baseFolderName))
                .map(rv -> Response.ok(rv).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("Error retrieving rendering info from " + baseFolderName + " or no folder found"))
                        .build())
                ;
    }

    @ApiOperation(value = "Get sample tile", notes = "Returns the requested TM sample tile at the specified zoom level")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred")})
    @GET
    @Path("samples/{sampleId}/rendering/tile")
    @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
    public Response streamTileFromCoord(
            @PathParam("sampleId") Long sampleId,
            @QueryParam("zoom") Integer zoomParam,
            @QueryParam("axis") CoordinateAxis axisParam,
            @QueryParam("x") Integer xParam,
            @QueryParam("y") Integer yParam,
            @QueryParam("z") Integer zParam) {
        logger.debug("Stream tile ({}, {}, {}, {}, {}) for {}", zoomParam, axisParam, xParam, yParam, zParam, sampleId);
        TmSample tmSample = tmSampleDao.findById(sampleId);
        if (tmSample == null) {
            logger.warn("No sample found for {}", sampleId);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No sample found for " + sampleId))
                    .build();
        }
        return streamTileFromDirAndCoord(
                tmSample.getFilepath(),
                zoomParam, axisParam, xParam,
                yParam,
                zParam
        );
    }

    @ApiOperation(value = "Get sample tile", notes = "Returns the requested TM sample tile at the specified zoom level")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred")})
    @GET
    @Path("rendering/tile/{baseFolder:.*}")
    @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
    public Response streamTileFromDirAndCoord(
            @PathParam("baseFolder") String baseFolderParam,
            @QueryParam("zoom") Integer zoomParam,
            @QueryParam("axis") CoordinateAxis axisParam,
            @QueryParam("x") Integer xParam,
            @QueryParam("y") Integer yParam,
            @QueryParam("z") Integer zParam) {
        logger.debug("Stream tile ({}, {}, {}, {}, {}) from {}", zoomParam, axisParam, xParam, yParam, zParam, baseFolderParam);
        if (StringUtils.isBlank(baseFolderParam)) {
            logger.warn("No base folder has been specified: {}", baseFolderParam);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse("No base path has been specified"))
                    .build();
        }
        String baseFolderName = StringUtils.prependIfMissing(baseFolderParam, "/");
        return renderedVolumeLoader.loadVolume(Paths.get(baseFolderName))
                .flatMap(rv -> rv.getTileInfo(axisParam)
                        .map(tileInfo -> TileKey.fromRavelerTileCoord(
                                xParam,
                                yParam,
                                zParam,
                                zoomParam,
                                axisParam,
                                tileInfo))
                        .flatMap(tileKey -> {
                            logger.debug("Load tile {} ({}, {}, {}, {}, {}) from {}",
                                    tileKey, zoomParam, axisParam, xParam, yParam, zParam, baseFolderName);
                            return renderedVolumeLoader.loadSlice(rv, tileKey);
                        }))
                .map(sliceImageBytes -> {
                    StreamingOutput sliceImageStream = output -> {
                        output.write(sliceImageBytes);
                    };
                    return sliceImageStream;
                })
                .map(sliceImageStream -> Response
                        .ok(sliceImageStream, MediaType.APPLICATION_OCTET_STREAM)
                        .build())
                .orElseGet(() -> Response.status(Response.Status.NO_CONTENT).build());
    }

    @Deprecated
    @ApiOperation(value = "Get sample tile", notes = "Returns the requested TM sample tile at the specified zoom level")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred")})
    @GET
    @Path("sample2DTile")
    @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
    public Response deprecatedStreamTileFromCoord(
            @QueryParam("sampleId") Long sampleId,
            @QueryParam("x") Integer xParam,
            @QueryParam("y") Integer yParam,
            @QueryParam("z") Integer zParam,
            @QueryParam("zoom") Integer zoomParam,
            @QueryParam("maxZoom") Integer maxZoomParam,
            @QueryParam("rendering_type") RenderingType renderingType,
            @QueryParam("axis") CoordinateAxis axisParam) {
        logger.debug("Stream 2D tile ({}, {}, {}, {}, {}) for {}", zoomParam, axisParam, xParam, yParam, zParam, sampleId);
        TmSample tmSample = tmSampleDao.findById(sampleId);
        if (tmSample == null) {
            logger.warn("No sample found for {}", sampleId);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No sample found for " + sampleId))
                    .build();
        }
        return streamTileFromDirAndCoord(
                tmSample.getFilepath(),
                zoomParam, axisParam, xParam,
                yParam,
                zParam
        );
    }

    @ApiOperation(value = "Tiff Stream", notes = "Streams the requested tile stored as a TIFF file")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred")})
    @GET
    @Path("mouseLightTiffStream")
    @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
    public Response streamTIFFTile(@QueryParam("suggestedPath") String pathHint) {
        return getTileTIFFFile(pathHint)
                .map(tileFile -> {
                    StreamingOutput tileStream = output -> {
                        Files.copy(tileFile.toPath(), output);
                    };
                    return Response.ok(tileStream, MediaType.APPLICATION_OCTET_STREAM)
                            .header("Content-Length", tileFile.length())
                            .build();
                })
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No file found for " + pathHint))
                        .build())
                ;
    }

    private Optional<File> getTileTIFFFile(String pathHint) {
        File candidateFile = new File(pathHint);
        if (candidateFile.exists()) {
            return Optional.of(candidateFile);
        } else {
            String candidateFileName = candidateFile.getName();
            int extensionSeparatorPos = candidateFileName.lastIndexOf(".");
            String candidateFileExt;
            if (extensionSeparatorPos > -1) {
                candidateFileExt = candidateFileName.substring(extensionSeparatorPos);
            } else {
                candidateFileExt = "";
            }
            File parentDir = candidateFile.getParentFile();
            if (!parentDir.isDirectory()) {
                return Optional.empty();
            } else {
                File[] childFiles = parentDir.listFiles();
                if (childFiles == null || childFiles.length == 0) {
                    logger.warn("Parent of suggested path {} - {} is not a directory", pathHint, parentDir);
                    return Optional.empty();
                }
                Predicate<File> filterPredicate;
                if (StringUtils.isBlank(candidateFileExt)) {
                    filterPredicate = f -> f.getName().endsWith(candidateFileExt);
                } else {
                    filterPredicate = f -> f.getName().endsWith(".tif") || f.getName().endsWith(".tiff");
                }
                return Stream.of(childFiles)
                        .filter(filterPredicate)
                        .findFirst();
            }
        }
    }
}