package org.janelia.jacs2.dataservice.rendering;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.rendering.JADEBasedRenderedVolumeLocation;
import org.janelia.rendering.RenderedVolumeLocation;
import org.janelia.rendering.utils.HttpClientProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.nio.file.Paths;
import java.util.Optional;

public class RenderedVolumeLocationFactory {
    private final static Logger LOG = LoggerFactory.getLogger(RenderedVolumeLocationFactory.class);

    private final StorageService storageService;
    private final HttpClientProvider httpClientProvider;
    private final String storageServiceApiKey;

    @Inject
    public RenderedVolumeLocationFactory(StorageService storageService,
                                         HttpClientProvider httpClientProvider,
                                         @PropertyValue(name = "StorageService.ApiKey") String storageServiceApiKey) {
        this.storageService = storageService;
        this.httpClientProvider = httpClientProvider;
        this.storageServiceApiKey = storageServiceApiKey;
    }

    public RenderedVolumeLocation getVolumeLocation(String samplePath, String subjectKey, String authToken) {
        Preconditions.checkArgument(StringUtils.isNotBlank(samplePath));
        return storageService.lookupDataStorage(null, null, null, samplePath, subjectKey, authToken)
                .map(dsInfo -> Optional.<RenderedVolumeLocation>of(new JADEBasedRenderedVolumeLocation(dsInfo.getDataStorageURI(), "", authToken, storageServiceApiKey, httpClientProvider)))
                .orElseGet(() -> storageService.lookupStorageVolumes(null, null, samplePath, subjectKey, authToken)
                            .map(vsInfo -> {
                                String renderedVolumePath;
                                if (samplePath.startsWith(vsInfo.getStorageVirtualPath())) {
                                    renderedVolumePath = Paths.get(vsInfo.getStorageVirtualPath()).relativize(Paths.get(samplePath)).toString();
                                } else {
                                    renderedVolumePath = Paths.get(vsInfo.getBaseStorageRootDir()).relativize(Paths.get(samplePath)).toString();
                                }
                                LOG.info("Create JADE volume location with URL {} and volume path {}", vsInfo.getStorageURL(), renderedVolumePath);
                                return new JADEBasedRenderedVolumeLocation(vsInfo.getStorageURL(),
                                        renderedVolumePath,
                                        authToken,
                                        storageServiceApiKey,
                                        httpClientProvider);
                            }))
                .orElseThrow(() -> new IllegalArgumentException("No volume location could be created for sample at " + samplePath))
                ;
    }

}
