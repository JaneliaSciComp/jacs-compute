package org.janelia.jacs2.asyncservice.dataimport;

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

    ServiceComputation<JacsServiceResult<List<StorageContentInfo>>> listContent(JacsServiceData jacsServiceData, String storageURL) {
        return computationFactory.<JacsServiceResult<List<StorageContentInfo>>>newComputation()
                .supply(() -> {
                    List<StorageService.StorageEntryInfo> contentToLoad = storageService.listStorageContent(
                            storageURL,
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
                                        Path entryRelativePath = Paths.get(contentEntry.getRemoteInfo().getEntryRelativePath());
                                        Path localEntryFullPath = localBasePath.resolve(entryRelativePath);
                                        if (Files.notExists(localEntryFullPath) || Files.size(localEntryFullPath) == 0) {
                                            // no local copy found - so download it
                                            Files.createDirectories(localEntryFullPath.getParent());
                                            Files.copy(
                                                    storageService.getStorageContent(
                                                            contentEntry.getRemoteInfo().getStorageURL(),
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
                                            contentEntry.setRemoteInfo(storageService.putStorageContent(
                                                    storageURL,
                                                    contentEntry.getLocalRelativePath().toString(),
                                                    jacsServiceData.getOwnerKey(),
                                                    ResourceHelper.getAuthToken(jacsServiceData.getResources()),
                                                    inputStream
                                            ));
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
