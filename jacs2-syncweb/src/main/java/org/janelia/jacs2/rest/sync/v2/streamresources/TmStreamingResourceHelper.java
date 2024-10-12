package org.janelia.jacs2.rest.sync.v2.streamresources;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.dataservice.storage.DataStorageLocationFactory;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.jacsstorage.clients.api.JadeStorageAttributes;
import org.janelia.rendering.Coordinate;
import org.janelia.rendering.RenderedVolumeLoader;
import org.janelia.rendering.TileKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TmStreamingResourceHelper {

    private static final Logger LOG = LoggerFactory.getLogger(TmStreamingResourceHelper.class);

    static Response streamTileFromDirAndCoord(
            DataStorageLocationFactory dataStorageLocationFactory,
            RenderedVolumeLoader renderedVolumeLoader,
            String subjectKey,
            String baseFolderParam,
            Integer zoomParam,
            Coordinate axisParam,
            Integer xParam,
            Integer yParam,
            Integer zParam,
            JadeStorageAttributes storageAttributes) {
        LOG.debug("Stream tile ({}, {}, {}, {}, {}) from {}", zoomParam, axisParam, xParam, yParam, zParam, baseFolderParam);
        if (StringUtils.isBlank(baseFolderParam)) {
            LOG.error("No base folder has been specified: {}", baseFolderParam);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse("No base path has been specified"))
                    .build();
        }
        String baseFolderName = StringUtils.prependIfMissing(baseFolderParam, "/");
        return dataStorageLocationFactory.lookupJadeDataLocation(baseFolderName, subjectKey, null, storageAttributes)
                .map(dl -> dataStorageLocationFactory.asRenderedVolumeLocation(dl))
                .flatMap(rvl -> renderedVolumeLoader.loadVolume(rvl)
                                .flatMap(rvm -> rvm.getTileInfo(axisParam)
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
                                            return renderedVolumeLoader.loadSlice(rvl, rvm, tileKey).asOptional();
                                        })))
                .map(sliceBytes -> {
                    StreamingOutput outputStreaming = output -> {
                        output.write(sliceBytes);
                    };
                    return Response
                            .ok(outputStreaming, MediaType.APPLICATION_OCTET_STREAM)
                            .header("Content-Length", sliceBytes.length)
                            .build();

                })
                .orElseGet(() -> Response.status(Response.Status.NO_CONTENT).build())
                ;
    }

}
