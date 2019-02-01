package org.janelia.jacs2.dataservice.rendering;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.rendering.FileBasedRenderedVolumeLocation;
import org.janelia.rendering.JADEBasedRenderedVolumeLocation;
import org.janelia.rendering.RenderedVolumeLocation;

import javax.inject.Inject;
import java.nio.file.Files;
import java.nio.file.Paths;

public class RenderedVolumeLocationFactory {
    private final StorageService storageService;

    @Inject
    public RenderedVolumeLocationFactory(StorageService storageService) {
        this.storageService = storageService;
    }

    public RenderedVolumeLocation getVolumeLocation(String samplePath) {
        Preconditions.checkArgument(StringUtils.isNotBlank(samplePath));
        if (Files.exists(Paths.get(samplePath))) {
            return new FileBasedRenderedVolumeLocation(Paths.get(samplePath));
        } else {
            return storageService.lookupStorage(null, null, null, samplePath, null, null)
                    .map(dsInfo -> new JADEBasedRenderedVolumeLocation(dsInfo.getDataStorageURI(), null))
                    .orElseThrow(() -> new IllegalArgumentException("No volume location could be created for sample at " + samplePath))
                    ;
        }
    }

}
