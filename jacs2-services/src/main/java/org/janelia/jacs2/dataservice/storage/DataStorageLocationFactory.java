package org.janelia.jacs2.dataservice.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.function.Function;

import javax.inject.Inject;

import com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.rendering.DataLocation;
import org.janelia.rendering.FileBasedDataLocation;
import org.janelia.rendering.FileBasedRenderedVolumeLocation;
import org.janelia.rendering.JADEBasedDataLocation;
import org.janelia.rendering.JADEBasedRenderedVolumeLocation;
import org.janelia.rendering.RenderedVolumeLocation;
import org.janelia.rendering.utils.ClientProxy;
import org.janelia.rendering.utils.HttpClientProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStorageLocationFactory {
    private final static Logger LOG = LoggerFactory.getLogger(DataStorageLocationFactory.class);

    private final StorageService storageService;
    private final HttpClientProvider httpClientProvider;
    private final String storageServiceApiKey;

    @Inject
    public DataStorageLocationFactory(StorageService storageService,
                                      HttpClientProvider httpClientProvider,
                                      @PropertyValue(name = "StorageService.ApiKey") String storageServiceApiKey) {
        this.storageService = storageService;
        this.httpClientProvider = httpClientProvider;
        this.storageServiceApiKey = storageServiceApiKey;
    }

    public DataLocation getDataLocationWithLocalCheck(String dataPath, String subjectKey, String authToken) {
        Path sampleLocalPath = Paths.get(dataPath);
        if (Files.exists(sampleLocalPath)) {
            return new FileBasedRenderedVolumeLocation(Paths.get(dataPath), Function.identity());
        } else {
            return lookupJadeDataLocation(dataPath, subjectKey, authToken)
                    .orElseThrow(() -> {
                        LOG.info("No storage location could be found for {}", dataPath);
                        return new IllegalArgumentException("No storage location could be found for  " + dataPath);
                    });
        }
    }

    public Optional<DataLocation> lookupJadeDataLocation(String dataPath, String subjectKey, String authToken) {
        Preconditions.checkArgument(StringUtils.isNotBlank(dataPath));
        return storageService.lookupDataStorage(null, null, null, dataPath, subjectKey, authToken)
                .map(dsInfo -> Optional.<DataLocation>of(new JADEBasedDataLocation(dsInfo.getConnectionURL(), dsInfo.getDataStorageURI(), "", authToken, storageServiceApiKey, httpClientProvider)))
                .orElseGet(() -> storageService.lookupStorageVolumes(null, null, dataPath, subjectKey, authToken)
                            .map(vsInfo -> {
                                String renderedVolumePath;
                                if (dataPath.startsWith(vsInfo.getStorageVirtualPath())) {
                                    renderedVolumePath = Paths.get(vsInfo.getStorageVirtualPath()).relativize(Paths.get(dataPath)).toString();
                                } else {
                                    renderedVolumePath = Paths.get(vsInfo.getBaseStorageRootDir()).relativize(Paths.get(dataPath)).toString();
                                }
                                LOG.debug("Create JADE volume location with URLs {}, {} and volume path {}", vsInfo.getStorageServiceURL(), vsInfo.getVolumeStorageURI(), renderedVolumePath);
                                return new JADEBasedDataLocation(
                                        vsInfo.getStorageServiceURL(),
                                        vsInfo.getVolumeStorageURI(),
                                        renderedVolumePath,
                                        authToken,
                                        storageServiceApiKey,
                                        httpClientProvider);
                            }))
                ;
    }

    public RenderedVolumeLocation asRenderedVolumeLocation(DataLocation dl) {
        if (dl == null) {
            return null;
        } else if (RenderedVolumeLocation.class.isAssignableFrom(dl.getClass())) {
            return (RenderedVolumeLocation) dl;
        } else if (dl instanceof JADEBasedDataLocation) {
            return new JADEBasedRenderedVolumeLocation((JADEBasedDataLocation) dl);
        } else if (dl instanceof FileBasedDataLocation) {
            return new FileBasedRenderedVolumeLocation((FileBasedDataLocation) dl);
        } else {
            LOG.warn("Unsupported data location type: {}", dl);
            throw new IllegalArgumentException("Unsupported data location type " + dl);
        }
    }
}
