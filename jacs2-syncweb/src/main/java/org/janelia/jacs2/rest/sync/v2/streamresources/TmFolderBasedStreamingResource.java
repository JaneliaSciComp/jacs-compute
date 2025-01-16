package org.janelia.jacs2.rest.sync.v2.streamresources;

import java.io.File;
import java.nio.file.Files;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.auth.JacsSecurityContextHelper;
import org.janelia.jacs2.dataservice.storage.DataStorageLocationFactory;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.jacsstorage.clients.api.JadeStorageAttributes;
import org.janelia.rendering.Coordinate;
import org.janelia.rendering.RenderedVolumeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
@Produces("application/json")
@Path("/mouselight")
public class TmFolderBasedStreamingResource {
    private static final Logger LOG = LoggerFactory.getLogger(TmFolderBasedStreamingResource.class);

    @Inject
    private DataStorageLocationFactory dataStorageLocationFactory;
    @Inject
    private RenderedVolumeLoader renderedVolumeLoader;

    @Operation(summary = "Get sample rendering info", description = "Retrieve volume rendering info for the specified base folder")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Success"),
            @ApiResponse(responseCode = "404", description = "Sample not found or no rendering"),
            @ApiResponse(responseCode = "500", description = "Error occurred")})
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("volume_info/{baseFolder:.*}")
    public Response getVolumeInfoFromBaseFolder(@PathParam("baseFolder") String baseFolderParam,
                                                @Context ContainerRequestContext requestContext) {
        if (StringUtils.isBlank(baseFolderParam)) {
            LOG.warn("No base folder has been specified: {}", baseFolderParam);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse("No base path has been specified"))
                    .build();
        }
        JadeStorageAttributes storageAttributes = new JadeStorageAttributes()
                .setAttributeValue("AccessKey", requestContext.getHeaderString("AccessKey"))
                .setAttributeValue("SecretKey", requestContext.getHeaderString("SecretKey"))
                .setAttributeValue("AWSRegion", requestContext.getHeaderString("AWSRegion"));
        return dataStorageLocationFactory.lookupJadeDataLocation(baseFolderParam, JacsSecurityContextHelper.getAuthorizedSubjectKey(requestContext), null, storageAttributes)
                .map(dl -> dataStorageLocationFactory.asRenderedVolumeLocation(dl))
                .flatMap(rvl -> renderedVolumeLoader.loadVolume(rvl))
                .map(rv -> Response.ok(rv).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("Error retrieving rendering info from " + baseFolderParam + " or no folder found"))
                        .build())
                ;
    }

    @Operation(
            summary = "Find closest tile info from voxel coordinates",
            description = "Retrieve info about the closest tile to the specified voxel")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Success"),
            @ApiResponse(responseCode = "404", description = "Base folder not found"),
            @ApiResponse(responseCode = "500", description = "Error occurred")})
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("closest_raw_tile_info/{baseFolder:.*}")
    public Response findClosestRawImageFromVoxelCoord(@PathParam("baseFolder") String baseFolderParam,
                                                      @QueryParam("x") Integer xVoxelParam,
                                                      @QueryParam("y") Integer yVoxelParam,
                                                      @QueryParam("z") Integer zVoxelParam,
                                                      @Context ContainerRequestContext requestContext) {
        if (StringUtils.isBlank(baseFolderParam)) {
            LOG.warn("No base folder has been specified: {}", baseFolderParam);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse("No base path has been specified"))
                    .build();
        }
        int xVoxel = xVoxelParam == null ? 0 : xVoxelParam;
        int yVoxel = yVoxelParam == null ? 0 : yVoxelParam;
        int zVoxel = zVoxelParam == null ? 0 : zVoxelParam;
        JadeStorageAttributes storageAttributes = new JadeStorageAttributes()
                .setAttributeValue("AccessKey", requestContext.getHeaderString("AccessKey"))
                .setAttributeValue("SecretKey", requestContext.getHeaderString("SecretKey"))
                .setAttributeValue("AWSRegion", requestContext.getHeaderString("AWSRegion"));
        return dataStorageLocationFactory.lookupJadeDataLocation(baseFolderParam, JacsSecurityContextHelper.getAuthorizedSubjectKey(requestContext), null, storageAttributes)
                .map(dl -> dataStorageLocationFactory.asRenderedVolumeLocation(dl))
                .flatMap(rvl -> renderedVolumeLoader.findClosestRawImageFromVoxelCoord(rvl, xVoxel, yVoxel, zVoxel))
                .map(rawTileImage -> Response.ok(rawTileImage).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("Error retrieving raw tile file info from " + baseFolderParam + " with ("
                                + xVoxelParam + "," + yVoxelParam + "," + zVoxelParam + ")"))
                        .build())
                ;
    }

    @Operation(
            summary = "Find closest tile info from voxel coordinates",
            description = "Retrieve info about the closest tile to the specified voxel")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Success"),
            @ApiResponse(responseCode = "404", description = "Base folder not found"),
            @ApiResponse(responseCode = "500", description = "Error occurred")})
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM})
    @Path("closest_raw_tile_stream/{baseFolder:.*}")
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
            LOG.warn("No base folder has been specified: {}", baseFolderParam);
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
        JadeStorageAttributes storageAttributes = new JadeStorageAttributes()
                .setAttributeValue("AccessKey", requestContext.getHeaderString("AccessKey"))
                .setAttributeValue("SecretKey", requestContext.getHeaderString("SecretKey"))
                .setAttributeValue("AWSRegion", requestContext.getHeaderString("AWSRegion"));
        return dataStorageLocationFactory.lookupJadeDataLocation(baseFolderParam, JacsSecurityContextHelper.getAuthorizedSubjectKey(requestContext), null, storageAttributes)
                .map(dl -> dataStorageLocationFactory.asRenderedVolumeLocation(dl))
                .flatMap(rvl -> renderedVolumeLoader.findClosestRawImageFromVoxelCoord(rvl, xVoxel, yVoxel, zVoxel)
                        .flatMap(rawTileImage -> renderedVolumeLoader.loadRawImageContentFromVoxelCoord(rvl, rawTileImage, channel, xVoxel, yVoxel, zVoxel, sx, sy, sz).asOptional()))
                .map(rawImageContent -> {
                    StreamingOutput outputStreaming = output -> {
                        output.write(rawImageContent);
                    };
                    return Response
                            .ok(outputStreaming, MediaType.APPLICATION_OCTET_STREAM)
                            .header("Content-Length", rawImageContent.length)
                            .build();
                })
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("Error retrieving raw tile file info from " + baseFolderParam + " with ("
                                + xVoxelParam + "," + yVoxelParam + "," + zVoxelParam + ")"))
                        .build())
                ;
    }

    @Operation(summary = "Get sample tile", description = "Returns the requested TM sample tile at the specified zoom level")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Success"),
            @ApiResponse(responseCode = "500", description = "Error occurred")})
    @GET
    @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
    @Path("rendering/tile/{baseFolder:.*}")
    public Response streamTileFromDirAndCoord(
            @PathParam("baseFolder") String baseFolderParam,
            @QueryParam("zoom") Integer zoomParam,
            @QueryParam("axis") Coordinate axisParam,
            @QueryParam("x") Integer xParam,
            @QueryParam("y") Integer yParam,
            @QueryParam("z") Integer zParam,
            @Context ContainerRequestContext requestContext) {
        JadeStorageAttributes storageAttributes = new JadeStorageAttributes()
                .setAttributeValue("AccessKey", requestContext.getHeaderString("AccessKey"))
                .setAttributeValue("SecretKey", requestContext.getHeaderString("SecretKey"))
                .setAttributeValue("AWSRegion", requestContext.getHeaderString("AWSRegion"));
        return TmStreamingResourceHelper.streamTileFromDirAndCoord(
                dataStorageLocationFactory, renderedVolumeLoader,
                JacsSecurityContextHelper.getAuthorizedSubjectKey(requestContext),
                baseFolderParam, zoomParam, axisParam, xParam, yParam, zParam,
                storageAttributes);
    }

    @Operation(summary = "Tiff Stream", description = "Streams the requested tile stored as a TIFF file")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Success"),
            @ApiResponse(responseCode = "500", description = "Error occurred")})
    @GET
    @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
    @Path("mouseLightTiffStream")
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
                    LOG.warn("Parent of suggested path {} - {} is not a directory", pathHint, parentDir);
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
