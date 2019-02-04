package org.janelia.jacs2.dataservice.rendering;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.rendering.FileBasedRenderedVolumeLocation;
import org.janelia.rendering.JADEBasedRenderedVolumeLocation;
import org.janelia.rendering.RenderedVolumeLocation;

import javax.inject.Inject;
import java.nio.file.Files;
import java.nio.file.Paths;

public class RenderedVolumeLocationFactory {
    private final StorageService storageService;
    private final String storageServiceApiKey;

    @Inject
    public RenderedVolumeLocationFactory(StorageService storageService,
                                         @PropertyValue(name = "StorageService.ApiKey") String storageServiceApiKey) {
        this.storageService = storageService;
        this.storageServiceApiKey = storageServiceApiKey;
    }

    public RenderedVolumeLocation getVolumeLocation(String samplePath, String subjectKey, String authToken) {
        Preconditions.checkArgument(StringUtils.isNotBlank(samplePath));
        if (Files.exists(Paths.get(samplePath))) {
            return new FileBasedRenderedVolumeLocation(Paths.get(samplePath));
        } else {
            return storageService.lookupStorage(null, null, null, samplePath, subjectKey, authToken)
                    .map(dsInfo -> new JADEBasedRenderedVolumeLocation(dsInfo.getDataStorageURI(), authToken, storageServiceApiKey))
                    .orElseThrow(() -> new IllegalArgumentException("No volume location could be created for sample at " + samplePath))
                    ;
        }
    }

}
