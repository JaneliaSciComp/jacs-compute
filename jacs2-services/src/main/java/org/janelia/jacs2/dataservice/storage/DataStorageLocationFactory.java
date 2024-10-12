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
import org.janelia.jacsstorage.clients.api.JadeStorageAttributes;
import org.janelia.jacsstorage.clients.api.rendering.JadeBasedDataLocation;
import org.janelia.jacsstorage.clients.api.rendering.JadeBasedRenderedVolumeLocation;
import org.janelia.rendering.DataLocation;
import org.janelia.rendering.FileBasedDataLocation;
import org.janelia.rendering.FileBasedRenderedVolumeLocation;
import org.janelia.rendering.RenderedVolumeLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStorageLocationFactory {
    private final static Logger LOG = LoggerFactory.getLogger(DataStorageLocationFactory.class);

    private final StorageService storageService;
    private final String storageServiceApiKey;

    @Inject
    public DataStorageLocationFactory(StorageService storageService,
                                      @PropertyValue(name = "StorageService.ApiKey") String storageServiceApiKey) {
        this.storageService = storageService;
        this.storageServiceApiKey = storageServiceApiKey;
    }

    public DataLocation getDataLocationWithLocalCheck(String dataPath, String subjectKey, String authToken,JadeStorageAttributes storageAttributes) {
        Path sampleLocalPath = Paths.get(dataPath);
        if (Files.exists(sampleLocalPath)) {
            return new FileBasedRenderedVolumeLocation(Paths.get(dataPath), Function.identity());
        } else {
            return lookupJadeDataLocation(dataPath, subjectKey, authToken, storageAttributes)
                    .orElseThrow(() -> {
                        LOG.info("No storage location could be found for {}", dataPath);
                        return new IllegalArgumentException("No storage location could be found for  " + dataPath);
                    });
        }
    }

    public Optional<? extends DataLocation> lookupJadeDataLocation(String dataPath, String subjectKey, String authToken, JadeStorageAttributes storageAttributes) {
        Preconditions.checkArgument(StringUtils.isNotBlank(dataPath));
        LOG.debug("lookupJadeDataLocation {}", dataPath);
        return storageService.lookupDataBundle(null, null, null, dataPath, subjectKey, authToken, storageAttributes).stream()
                .map(dsInfo -> new JadeBasedDataLocation(dsInfo.getConnectionURL(), dsInfo.getDataStorageURI(), "", authToken, storageServiceApiKey, storageAttributes))
                .filter(dataLocation -> {
                    if (dataPath.startsWith("/")) {
                        return dataLocation.checkContentAtAbsolutePath(dataPath);
                    } else {
                        return dataLocation.checkContentAtRelativePath(dataPath);
                    }
                })
                .findFirst()
                .map(Optional::of)
                .orElseGet(() -> searchStorageVolumes(dataPath, subjectKey, authToken, storageAttributes))
                ;
    }

    /**
     * Search JADE volumes.
     * @param dataPath
     * @param subjectKey
     * @param authToken
     * @return
     */
    private Optional<JadeBasedDataLocation> searchStorageVolumes(String dataPath, String subjectKey, String authToken, JadeStorageAttributes storageAttributes) {
        LOG.debug("searchStorageVolumes {}", dataPath);
        return storageService.findStorageVolumes(dataPath, subjectKey, authToken, storageAttributes).stream()
                .map(vsInfo -> {
                    String renderedVolumePath;
                    if (vsInfo.getStorageVirtualPath() != null && dataPath.startsWith(StringUtils.appendIfMissing(vsInfo.getStorageVirtualPath(), "/"))) {
                        renderedVolumePath = Paths.get(vsInfo.getStorageVirtualPath()).relativize(Paths.get(dataPath)).toString();
                    } else if (vsInfo.getBaseStorageRootDir() != null && dataPath.startsWith(StringUtils.appendIfMissing(vsInfo.getBaseStorageRootDir(), "/"))) {
                        renderedVolumePath = Paths.get(vsInfo.getBaseStorageRootDir()).relativize(Paths.get(dataPath)).toString();
                    } else if ("S3".equals(vsInfo.getStorageType())) {
                        // S3 volumes may have null root dir - in that case renderedVolume
                        // is located at the same path as the one used for searching the volume, i.e. dataPath
                        renderedVolumePath = dataPath;
                    } else {
                        // the only other option is that the dataPath is actually the root volume path
                        renderedVolumePath = "";
                    }
                    LOG.debug("Create JADE volume location with URLs {}, {} and volume path {}", vsInfo.getStorageServiceURL(), vsInfo.getVolumeStorageURI(), renderedVolumePath);
                    return new JadeBasedDataLocation(
                            vsInfo.getStorageServiceURL(),
                            vsInfo.getVolumeStorageURI(),
                            renderedVolumePath,
                            authToken,
                            storageServiceApiKey,
                            storageAttributes);
                })
                .filter(dl -> dl.checkContentAtAbsolutePath(dataPath))
                .findFirst()
                ;
    }

    public RenderedVolumeLocation asRenderedVolumeLocation(DataLocation dl) {
        if (dl == null) {
            return null;
        } else if (RenderedVolumeLocation.class.isAssignableFrom(dl.getClass())) {
            return (RenderedVolumeLocation) dl;
        } else if (dl instanceof JadeBasedDataLocation) {
            return new JadeBasedRenderedVolumeLocation((JadeBasedDataLocation) dl);
        } else if (dl instanceof FileBasedDataLocation) {
            return new FileBasedRenderedVolumeLocation((FileBasedDataLocation) dl);
        } else {
            LOG.warn("Unsupported data location type: {}", dl);
            throw new IllegalArgumentException("Unsupported data location type " + dl);
        }
    }
}
