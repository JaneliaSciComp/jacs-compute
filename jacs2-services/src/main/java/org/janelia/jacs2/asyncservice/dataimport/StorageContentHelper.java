package org.janelia.jacs2.asyncservice.dataimport;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ResourceHelper;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.model.service.JacsServiceData;
import org.slf4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Helper class for downloading/uploading content to JADE.
 */
class StorageContentHelper {

    private final ServiceComputationFactory computationFactory;
    private final StorageService storageService;
    private final Logger logger;

    StorageContentHelper(ServiceComputationFactory computationFactory,
                         StorageService storageService,
                         Logger logger) {
        this.computationFactory = computationFactory;
        this.storageService = storageService;
        this.logger = logger;
    }

    StorageService.StorageInfo getOrCreateStorage(String storageServiceURL, String storageId, String storageName, String ownerKey, String authToken) {
        return storageService
                .lookupStorage(storageServiceURL, storageId, storageName, ownerKey, authToken)
                .orElseGet(() -> storageService.createStorage(storageServiceURL, storageName, ownerKey, authToken))
                ;
    }

    ServiceComputation<JacsServiceResult<List<StorageContentInfo>>> listContent(JacsServiceData jacsServiceData, String storageURL, String storagePath) {
        return computationFactory.<JacsServiceResult<List<StorageContentInfo>>>newComputation()
                .supply(() -> {
                    List<StorageService.StorageEntryInfo> contentToLoad = storageService.listStorageContent(
                            storageURL,
                            storagePath,
                            jacsServiceData.getOwnerKey(),
                            ResourceHelper.getAuthToken(jacsServiceData.getResources()));
                    return new JacsServiceResult<>(jacsServiceData,
                            contentToLoad.stream()
                                    .filter(entry -> !entry.isCollectionFlag())
                                    .map(entry -> {
                                        StorageContentInfo storageContentInfo = new StorageContentInfo();
                                        storageContentInfo.setRemoteInfo(entry);
                                        return storageContentInfo;
                                    })
                                    .collect(Collectors.toList())
                        );
                })
                ;
    }

    ServiceComputation<JacsServiceResult<List<StorageContentInfo>>> downloadContent(JacsServiceData jacsServiceData,
                                                                                    Path localBasePath,
                                                                                    List<StorageContentInfo> contentList) {
        return computationFactory.<JacsServiceResult<List<StorageContentInfo>>>newComputation()
                .supply(() -> new JacsServiceResult<>(
                        jacsServiceData,
                        contentList.stream()
                                .peek(contentEntry -> {
                                    try {
                                        String entryRelativePathName = contentEntry.getRemoteInfo().getEntryRelativePath();
                                        Path entryRelativePath = Paths.get(sanitizeFileName(entryRelativePathName));
                                        Path localEntryFullPath = localBasePath.resolve(entryRelativePath);
                                        if (Files.notExists(localEntryFullPath) || Files.size(localEntryFullPath) == 0) {
                                            // no local copy found - so download it
                                            Files.createDirectories(localEntryFullPath.getParent());
                                            Files.copy(
                                                    storageService.getStorageContent(
                                                            contentEntry.getRemoteInfo().getStorageEntryURL(),
                                                            contentEntry.getRemoteInfo().getEntryRelativePath(),
                                                            jacsServiceData.getOwnerKey(),
                                                            ResourceHelper.getAuthToken(jacsServiceData.getResources())),
                                                    localEntryFullPath,
                                                    StandardCopyOption.REPLACE_EXISTING);
                                        }
                                        // set local path info
                                        contentEntry.setLocalBasePath(localBasePath);
                                        contentEntry.setLocalRelativePath(entryRelativePath);
                                    } catch (IOException e) {
                                        throw new UncheckedIOException(e);
                                    }
                                })
                                .collect(Collectors.toList())
                ));
    }

    private String sanitizeFileName(String fname) {
        if (StringUtils.isBlank(fname)) {
            return fname;
        } else {
            return fname.replace('#', '_');
        }
    }

    ServiceComputation<JacsServiceResult<List<StorageContentInfo>>> uploadContent(JacsServiceData jacsServiceData, String storageURL, List<StorageContentInfo> contentList) {
        return computationFactory.<JacsServiceResult<List<StorageContentInfo>>>newComputation()
                .supply(() -> new JacsServiceResult<>(
                        jacsServiceData,
                        contentList.stream()
                                .peek(contentEntry -> {
                                    FileInputStream inputStream = null;
                                    try {
                                        Path localPath = contentEntry.getLocalBasePath().resolve(contentEntry.getLocalRelativePath());
                                        if (Files.exists(localPath)) {
                                            logger.info("Upload {}({}) to {}", contentEntry, localPath, storageURL);
                                            inputStream = new FileInputStream(localPath.toFile());
                                            if (StringUtils.isBlank(contentEntry.getRemoteInfo().getStorageId())) {
                                                // if the data is pushed directly to the volume because there's no bundle specified
                                                // use the full virtual path to push to "storage_path/file/{dataPath:.*}" endpoint
                                                contentEntry.setRemoteInfo(storageService.putStorageContent(
                                                        storageURL,
                                                        contentEntry.getRemoteInfo().getEntryRootPrefix() + "/" + contentEntry.getRemoteInfo().getEntryRelativePath(),
                                                        jacsServiceData.getOwnerKey(),
                                                        ResourceHelper.getAuthToken(jacsServiceData.getResources()),
                                                        inputStream
                                                ));
                                            } else {
                                                // if the data is pushed to an existing bundle use the entry relative path
                                                // storageURL should reference the bundle and the content will be pushed to
                                                // "{dataBundleId}/file/{dataEntryPath:.*}" endpoint
                                                contentEntry.setRemoteInfo(storageService.putStorageContent(
                                                        storageURL,
                                                        contentEntry.getRemoteInfo().getEntryRelativePath(),
                                                        jacsServiceData.getOwnerKey(),
                                                        ResourceHelper.getAuthToken(jacsServiceData.getResources()),
                                                        inputStream
                                                ));
                                            }
                                        }
                                    } catch (IOException e) {
                                        throw new UncheckedIOException(e);
                                    } finally {
                                        if (inputStream != null) {
                                            try {
                                                inputStream.close();
                                            } catch (IOException e) {
                                                // ignore
                                            }
                                        }
                                    }
                                })
                                .collect(Collectors.toList())
                ));
    }

}
