package org.janelia.jacs2.rest.sync.v2.streamresources;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.auth.JacsSecurityContextHelper;
import org.janelia.jacs2.dataservice.rendering.RenderedVolumeLocationFactory;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.rendering.Coordinate;
import org.janelia.rendering.RawImage;
import org.janelia.rendering.RenderedVolume;
import org.janelia.rendering.RenderedVolumeLoader;
import org.janelia.rendering.RenderedVolumeLocation;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.File;
import java.nio.file.Files;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

@ApplicationScoped
@Produces("application/json")
@Path("/mouselight")
public class TmFolderBasedStreamingResource {

    @Inject
    private RenderedVolumeLocationFactory renderedVolumeLocationFactory;
    @Inject
    private RenderedVolumeLoader renderedVolumeLoader;
    @Inject
    private Logger logger;


    @ApiOperation(value = "Get sample rendering info", notes = "Retrieve volume rendering info for the specified base folder")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success", response = RenderedVolume.class),
            @ApiResponse(code = 404, message = "Sample not found or no rendering"),
            @ApiResponse(code = 500, message = "Error occurred")})
    @GET
    @Path("volume_info/{baseFolder:.*}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getVolumeInfoFromBaseFolder(@PathParam("baseFolder") String baseFolderParam,
                                                @Context ContainerRequestContext requestContext) {
        if (StringUtils.isBlank(baseFolderParam)) {
            logger.warn("No base folder has been specified: {}", baseFolderParam);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse("No base path has been specified"))
                    .build();
        }
        String baseFolderName = StringUtils.prependIfMissing(baseFolderParam, "/");
        return renderedVolumeLoader.loadVolume(renderedVolumeLocationFactory.getVolumeLocation(baseFolderName,
                JacsSecurityContextHelper.getAuthorizedSubjectKey(requestContext),
                null))
                .map(rv -> Response.ok(rv).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("Error retrieving rendering info from " + baseFolderName + " or no folder found"))
                        .build())
                ;
    }

    @ApiOperation(
            value = "Find closest tile info from voxel coordinates",
            notes = "Retrieve info about the closest tile to the specified voxel")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success", response = RawImage.class),
            @ApiResponse(code = 404, message = "Base folder not found"),
            @ApiResponse(code = 500, message = "Error occurred")})
    @GET
    @Path("closest_raw_tile_info/{baseFolder:.*}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response findClosestRawImageFromVoxelCoord(@PathParam("baseFolder") String baseFolderParam,
                                                      @QueryParam("x") Integer xVoxelParam,
                                                      @QueryParam("y") Integer yVoxelParam,
                                                      @QueryParam("z") Integer zVoxelParam,
                                                      @Context ContainerRequestContext requestContext) {
        if (StringUtils.isBlank(baseFolderParam)) {
            logger.warn("No base folder has been specified: {}", baseFolderParam);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse("No base path has been specified"))
                    .build();
        }
        int xVoxel = xVoxelParam == null ? 0 : xVoxelParam;
        int yVoxel = yVoxelParam == null ? 0 : yVoxelParam;
        int zVoxel = zVoxelParam == null ? 0 : zVoxelParam;
        String baseFolderName = StringUtils.prependIfMissing(baseFolderParam, "/");
        return renderedVolumeLoader.findClosestRawImageFromVoxelCoord(
                renderedVolumeLocationFactory.getVolumeLocation(baseFolderName,
                        JacsSecurityContextHelper.getAuthorizedSubjectKey(requestContext),
                        null),
                xVoxel, yVoxel, zVoxel)
                .map(rawTileImage -> Response.ok(rawTileImage).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("Error retrieving raw tile file info from " + baseFolderName + " with ("
                                + xVoxelParam + "," + yVoxelParam + "," + zVoxelParam + ")"))
                        .build())
                ;
    }

    @ApiOperation(
            value = "Find closest tile info from voxel coordinates",
            notes = "Retrieve info about the closest tile to the specified voxel")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 404, message = "Base folder not found"),
            @ApiResponse(code = 500, message = "Error occurred")})
    @GET
    @Path("closest_raw_tile_stream/{baseFolder:.*}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM})
    public Response streamClosestRawImageFromVoxelCoord(@PathParam("baseFolder") String baseFolderParam,
                                                        @QueryParam("x") Integer xVoxelParam,
                                                        @QueryParam("y") Integer yVoxelParam,
                                                        @QueryParam("z") Integer zVoxelParam,
                                                        @QueryParam("sx") Integer sxParam,
                                                        @QueryParam("sy") Integer syParam,
                                                        @QueryParam("sz") Integer szParam,
                                                        @QueryParam("channel") Integer channelParam,
                                                        @Context ContainerRequestContext requestContext) {
        if (StringUtils.isBlank(baseFolderParam)) {
            logger.warn("No base folder has been specified: {}", baseFolderParam);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse("No base path has been specified"))
                    .build();
        }
        int xVoxel = xVoxelParam == null ? 0 : xVoxelParam;
        int yVoxel = yVoxelParam == null ? 0 : yVoxelParam;
        int zVoxel = zVoxelParam == null ? 0 : zVoxelParam;
        int sx = sxParam == null ? -1 : sxParam;
        int sy = syParam == null ? -1 : syParam;
        int sz = szParam == null ? -1 : szParam;
        int channel = channelParam == null ? 0 : channelParam;
        String baseFolderName = StringUtils.prependIfMissing(baseFolderParam, "/");
        RenderedVolumeLocation rvl = renderedVolumeLocationFactory.getVolumeLocation(baseFolderName,
                JacsSecurityContextHelper.getAuthorizedSubjectKey(requestContext),
                null);
        return renderedVolumeLoader.findClosestRawImageFromVoxelCoord(
                rvl,
                xVoxel, yVoxel, zVoxel)
                .map(rawTileImage -> {
                    byte[] rawImageBytes = renderedVolumeLoader.loadRawImageContentFromVoxelCoord(rvl, rawTileImage,
                            channel, xVoxel, yVoxel, zVoxel, sx, sy, sz);
                    if (rawImageBytes == null) {
                        return Response
                                .noContent()
                                .build();
                    } else {
                        StreamingOutput rawImageBytesStream = output -> {
                            output.write(rawImageBytes);
                        };
                        return Response
                                .ok(rawImageBytesStream, MediaType.APPLICATION_OCTET_STREAM)
                                .build();
                    }
                })
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("Error retrieving raw tile file info from " + baseFolderName + " with ("
                                + xVoxelParam + "," + yVoxelParam + "," + zVoxelParam + ")"))
                        .build())
                ;
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
            @QueryParam("axis") Coordinate axisParam,
            @QueryParam("x") Integer xParam,
            @QueryParam("y") Integer yParam,
            @QueryParam("z") Integer zParam,
            @Context ContainerRequestContext requestContext) {
        return TmStreamingResourceHelper.streamTileFromDirAndCoord(
                renderedVolumeLocationFactory, renderedVolumeLoader,
                JacsSecurityContextHelper.getAuthorizedSubjectKey(requestContext),
                baseFolderParam, zoomParam, axisParam, xParam, yParam, zParam);
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
