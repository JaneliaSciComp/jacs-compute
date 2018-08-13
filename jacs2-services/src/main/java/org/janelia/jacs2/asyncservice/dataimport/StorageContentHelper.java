package org.janelia.jacs2.asyncservice.dataimport;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ResourceHelper;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.imageservices.MIPsAndMoviesResult;
import org.janelia.jacs2.dataservice.storage.DataStorageInfo;
import org.janelia.jacs2.dataservice.storage.StorageEntryInfo;
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
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    DataStorageInfo getOrCreateStorage(String storageServiceURL, String storageId,
                                       String storageName, List<String> storageTags,
                                       String ownerKey, String authToken) {
        return storageService
                .lookupStorage(storageServiceURL, storageId, storageName, ownerKey, authToken)
                .orElseGet(() -> storageService.createStorage(storageServiceURL, storageName, storageTags, ownerKey, authToken))
                ;
    }

    ServiceComputation<JacsServiceResult<List<ContentStack>>> listContent(JacsServiceData jacsServiceData, String storageURL, String storagePath) {
        return computationFactory.<JacsServiceResult<List<ContentStack>>>newComputation()
                .supply(() -> {
                    List<StorageEntryInfo> contentToLoad = storageService.listStorageContent(
                            storageURL,
                            storagePath,
                            jacsServiceData.getOwnerKey(),
                            ResourceHelper.getAuthToken(jacsServiceData.getResources()));
                    return new JacsServiceResult<>(jacsServiceData,
                            contentToLoad.stream()
                                    .map(entry -> {
                                        StorageContentInfo storageContentInfo = new StorageContentInfo();
                                        storageContentInfo.setRemoteInfo(entry);
                                        return new ContentStack(storageContentInfo);
                                    })
                                    .collect(Collectors.toList())
                    );
                })
                ;
    }

    ServiceComputation<JacsServiceResult<List<ContentStack>>> downloadContent(JacsServiceData jacsServiceData,
                                                                              Path localBasePath,
                                                                              List<ContentStack> contentList) {
        return computationFactory.<JacsServiceResult<List<ContentStack>>>newComputation()
                .supply(() -> new JacsServiceResult<>(
                        jacsServiceData,
                        contentList.stream()
                                .peek(contentEntry -> {
                                    try {
                                        String entryRelativePathName = contentEntry.getMainRep().getRemoteInfo().getEntryRelativePath();
                                        Path entryRelativePath = Paths.get(sanitizeFileName(entryRelativePathName));
                                        Path localEntryFullPath = localBasePath.resolve(entryRelativePath);
                                        if (Files.notExists(localEntryFullPath) || Files.size(localEntryFullPath) == 0) {
                                            // no local copy found - so download it
                                            Files.createDirectories(localEntryFullPath.getParent());
                                            Files.copy(
                                                    storageService.getStorageContent(
                                                            contentEntry.getMainRep().getRemoteInfo().getEntryURL(),
                                                            contentEntry.getMainRep().getRemoteInfo().getEntryRelativePath(),
                                                            jacsServiceData.getOwnerKey(),
                                                            ResourceHelper.getAuthToken(jacsServiceData.getResources())),
                                                    localEntryFullPath,
                                                    StandardCopyOption.REPLACE_EXISTING);
                                        }
                                        // set local path info
                                        contentEntry.getMainRep().setLocalBasePath(localBasePath.toString());
                                        contentEntry.getMainRep().setLocalRelativePath(entryRelativePath.toString());
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

    void removeContent(JacsServiceData jacsServiceData, String storageURL, String storagePath) {
        storageService.removeStorageContent(
                storageURL,
                storagePath,
                jacsServiceData.getOwnerKey(),
                ResourceHelper.getAuthToken(jacsServiceData.getResources()));
    }

    ServiceComputation<JacsServiceResult<List<ContentStack>>> uploadContent(JacsServiceData jacsServiceData, String storageURL, List<ContentStack> contentList) {
        return computationFactory.<JacsServiceResult<List<ContentStack>>>newComputation()
                .supply(() -> new JacsServiceResult<>(
                        jacsServiceData,
                        contentList.stream()
                                .peek(contentEntry -> {
                                    Stream.concat(Stream.of(contentEntry.getMainRep()), contentEntry.getAdditionalReps().stream())
                                            .filter(sci -> sci.getRemoteInfo().getEntryURL() == null) // only upload the ones that don't have a storageEntryURL, i.e., not there yet
                                            .forEach(sci -> uploadContent(sci, storageURL, jacsServiceData.getOwnerKey(), ResourceHelper.getAuthToken(jacsServiceData.getResources())));
                                })
                                .collect(Collectors.toList())
                ));
    }

    List<ContentStack> addContentMips(List<ContentStack> contentList, Path localMIPSRootPath, List<MIPsAndMoviesResult> contentMips) {
        Map<String, ContentStack> indexedContent = contentList.stream()
                .filter(contentEntry -> contentEntry.getMainRep().getLocalFullPath() != null)
                .collect(Collectors.toMap(contentEntry -> contentEntry.getMainRep().getLocalFullPath().toString(), contentEntry -> contentEntry));
        contentMips.forEach(mipsAndMoviesResult -> {
            ContentStack inputStack = indexedContent.get(mipsAndMoviesResult.getFileInput());
            if (inputStack != null) {
                mipsAndMoviesResult.getFileList()
                        .forEach(mipsFile -> {
                            StorageContentInfo mipsContentInfo = new StorageContentInfo();
                            mipsContentInfo.setLocalBasePath(localMIPSRootPath.toString());
                            mipsContentInfo.setLocalRelativePath(localMIPSRootPath.relativize(Paths.get(mipsFile)).toString());
                            if (inputStack.getMainRep().getRemoteInfo() != null) {
                                mipsContentInfo.setRemoteInfo(new StorageEntryInfo(
                                        inputStack.getMainRep().getRemoteInfo().getStorageId(), // I want the MIPs in the same bundle if one exists
                                        inputStack.getMainRep().getRemoteInfo().getStorageURL(),
                                        null, // I don't know the entry URL yet
                                        inputStack.getMainRep().getRemoteInfo().getStorageRootLocation(),
                                        inputStack.getMainRep().getRemoteInfo().getStorageRootPathURI(),
                                        constructStorageEntryPath(mipsContentInfo, "mips"),
                                        false));
                            }
                            inputStack.addRepresentation(mipsContentInfo);
                        });
            }
        });
        return contentList;
    }

    String constructStorageEntryPath(StorageContentInfo storageContentInfo, String pathPrefix) {
        if (StringUtils.isBlank(pathPrefix)) {
            return storageContentInfo.getLocalRelativePath();
        } else {
            return Paths.get(pathPrefix, storageContentInfo.getLocalRelativePath()).toString();
        }
    }

    private void uploadContent(StorageContentInfo storageContentInfo, String storageURL, String subjectKey, String authToken) {
        FileInputStream inputStream = null;
        try {
            Path localPath = Paths.get(storageContentInfo.getLocalBasePath()).resolve(storageContentInfo.getLocalRelativePath());
            if (Files.exists(localPath)) {
                logger.info("Upload {}({}) to {}", storageContentInfo, localPath, storageURL);
                inputStream = new FileInputStream(localPath.toFile());
                if (StringUtils.isBlank(storageContentInfo.getRemoteInfo().getStorageId())) {
                    // if the data is pushed directly to the volume because there's no bundle specified
                    // use the full virtual path to push to "storage_path/file/{dataPath:.*}" endpoint
                    storageContentInfo.setRemoteInfo(storageService.putStorageContent(
                            storageURL,
                            storageContentInfo.getRemoteInfo().getStorageRootPathURI() + "/" + storageContentInfo.getRemoteInfo().getEntryRelativePath(),
                            subjectKey,
                            authToken,
                            inputStream
                    ));
                } else {
                    // if the data is pushed to an existing bundle use the entry relative path
                    // storageURL should reference the bundle and the content will be pushed to
                    // "{dataBundleId}/file/{dataEntryPath:.*}" endpoint
                    storageContentInfo.setRemoteInfo(storageService.putStorageContent(
                            storageURL,
                            storageContentInfo.getRemoteInfo().getEntryRelativePath(),
                            subjectKey,
                            authToken,
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

    }
}
