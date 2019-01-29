package org.janelia.jacs2.rest.sync.v2.streamresources;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.rendering.CoordinateAxis;
import org.janelia.rendering.RenderedVolumeLoader;
import org.janelia.rendering.TileKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.nio.file.Paths;

class TmStreamingResourceHelper {

    private static final Logger LOG = LoggerFactory.getLogger(TmStreamingResourceHelper.class);

    static Response streamTileFromDirAndCoord(
            RenderedVolumeLoader renderedVolumeLoader,
            String baseFolderParam,
            Integer zoomParam,
            CoordinateAxis axisParam,
            Integer xParam,
            Integer yParam,
            Integer zParam) {
        LOG.debug("Stream tile ({}, {}, {}, {}, {}) from {}", zoomParam, axisParam, xParam, yParam, zParam, baseFolderParam);
        if (StringUtils.isBlank(baseFolderParam)) {
            LOG.error("No base folder has been specified: {}", baseFolderParam);
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
                            LOG.debug("Load tile {} ({}, {}, {}, {}, {}) from {}",
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

}
