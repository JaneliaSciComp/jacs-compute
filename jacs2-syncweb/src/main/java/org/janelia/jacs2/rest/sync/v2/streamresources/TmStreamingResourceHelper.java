package org.janelia.jacs2.rest.sync.v2.streamresources;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.dataservice.rendering.RenderedVolumeLocationFactory;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.rendering.Coordinate;
import org.janelia.rendering.RenderedVolumeLoader;
import org.janelia.rendering.RenderedVolumeLocation;
import org.janelia.rendering.Streamable;
import org.janelia.rendering.TileKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import com.google.common.io.ByteStreams;

class TmStreamingResourceHelper {

    private static final Logger LOG = LoggerFactory.getLogger(TmStreamingResourceHelper.class);

    static Response streamTileFromDirAndCoord(
            RenderedVolumeLocationFactory renderedVolumeLocationFactory,
            RenderedVolumeLoader renderedVolumeLoader,
            String subjectKey,
            String baseFolderParam,
            Integer zoomParam,
            Coordinate axisParam,
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
        RenderedVolumeLocation rvl = renderedVolumeLocationFactory.getVolumeLocationWithLocalCheck(baseFolderName, subjectKey, null);
        Streamable<byte[]> volumeSlice = renderedVolumeLoader.loadVolume(rvl)
                .flatMap(rvm -> rvm.getTileInfo(axisParam)
                        .map(tileInfo -> TileKey.fromRavelerTileCoord(
                                xParam,
                                yParam,
                                zParam,
                                zoomParam,
                                axisParam,
                                tileInfo))
                        .map(tileKey -> {
                            LOG.debug("Load tile {} ({}, {}, {}, {}, {}) from {}",
                                    tileKey, zoomParam, axisParam, xParam, yParam, zParam, baseFolderName);
                            return renderedVolumeLoader.loadSlice(rvl, rvm, tileKey);
                        }))
                .orElse(Streamable.empty());
        if (volumeSlice.getContent() == null) {
            return Response.status(Response.Status.NO_CONTENT).build();
        } else {
            StreamingOutput outputStreaming = output -> {
                output.write(volumeSlice.getContent());
            };
            return Response
                    .ok(outputStreaming, MediaType.APPLICATION_OCTET_STREAM)
                    .header("Content-Length", volumeSlice.getSize())
                    .build();
        }
    }

}
