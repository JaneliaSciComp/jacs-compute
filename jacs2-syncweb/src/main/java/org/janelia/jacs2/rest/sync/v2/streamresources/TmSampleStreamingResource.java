package org.janelia.jacs2.rest.sync.v2.streamresources;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.domain.dao.TmSampleDao;
import org.janelia.model.domain.tiledMicroscope.TmSample;
import org.janelia.rendering.CoordinateAxis;
import org.janelia.rendering.RenderedVolume;
import org.janelia.rendering.RenderedVolumeLoader;
import org.janelia.rendering.RenderingType;
import org.janelia.rendering.cdi.WithCache;
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
import java.nio.file.Paths;

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
            @ApiResponse(code = 200, message = "Success", response = RenderedVolume.class),
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


    @ApiOperation(
            value = "Find closest tile info from voxel coordinates for the specified sample",
            notes = "Retrieve info about the closest tile to the specified voxel")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 404, message = "Sample not found"),
            @ApiResponse(code = 500, message = "Error occurred")})
    @GET
    @Path("samples/{sampleId}/closest_raw_tile_stream")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM})
    public Response streamClosestRawImageFromVoxelCoord(@PathParam("sampleId") Long sampleId,
                                                        @QueryParam("x") Integer xVoxelParam,
                                                        @QueryParam("y") Integer yVoxelParam,
                                                        @QueryParam("z") Integer zVoxelParam,
                                                        @QueryParam("sx") Integer sxParam,
                                                        @QueryParam("sy") Integer syParam,
                                                        @QueryParam("sz") Integer szParam,
                                                        @QueryParam("channel") Integer channelParam) {
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
        int xVoxel = xVoxelParam == null ? 0 : xVoxelParam;
        int yVoxel = yVoxelParam == null ? 0 : yVoxelParam;
        int zVoxel = zVoxelParam == null ? 0 : zVoxelParam;
        int sx = sxParam == null ? -1 : sxParam;
        int sy = syParam == null ? -1 : syParam;
        int sz = szParam == null ? -1 : szParam;
        int channel = channelParam == null ? 0 : channelParam;
        return renderedVolumeLoader.findClosestRawImageFromVoxelCoord(Paths.get(tmSample.getFilepath()), xVoxel, yVoxel, zVoxel)
                .map(rawTileImage -> {
                    byte[] rawImageBytes = renderedVolumeLoader.loadRawImageContentFromVoxelCoord(rawTileImage,
                            xVoxel, yVoxel, zVoxel, sx, sy, sz, channel);
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
                        .entity(new ErrorResponse("Error retrieving raw tile file info for sample " + sampleId + " with ("
                                + xVoxelParam + "," + yVoxelParam + "," + zVoxelParam + ")"))
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
        return TmStreamingResourceHelper.streamTileFromDirAndCoord(renderedVolumeLoader, tmSample.getFilepath(), zoomParam, axisParam, xParam, yParam, zParam);
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
        return TmStreamingResourceHelper.streamTileFromDirAndCoord(renderedVolumeLoader, tmSample.getFilepath(), zoomParam, axisParam, xParam, yParam, zParam);
    }

}
