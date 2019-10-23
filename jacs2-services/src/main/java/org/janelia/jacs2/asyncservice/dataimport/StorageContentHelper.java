package org.janelia.jacs2.asyncservice.dataimport;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ResourceHelper;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.imageservices.MIPsAndMoviesResult;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.dataservice.storage.DataStorageInfo;
import org.janelia.jacs2.dataservice.storage.JadeStorageVolume;
import org.janelia.jacs2.dataservice.storage.StorageEntryInfo;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.model.service.JacsServiceData;
import org.slf4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
                .lookupDataStorage(storageServiceURL, storageId, storageName, null, ownerKey, authToken)
                .orElseGet(() -> storageService.createStorage(storageServiceURL, storageName, storageTags, ownerKey, authToken))
                ;
    }

    Optional<JadeStorageVolume> lookupStorage(String storagePath, String ownerKey, String authToken) {
        return storageService.lookupStorageVolumes(null, null, storagePath, ownerKey, authToken);
    }

    ServiceComputation<JacsServiceResult<List<ContentStack>>> listContent(JacsServiceData jacsServiceData, String storageURL, String storagePath) {
        return computationFactory.<JacsServiceResult<List<ContentStack>>>newComputation()
                .supply(() -> {
                    List<StorageEntryInfo> contentToLoad = storageService.listStorageContent(
                            storageURL,
                            storagePath,
                            jacsServiceData.getOwnerKey(),
                            ResourceHelper.getAuthToken(jacsServiceData.getResources()),
                            -1,
                            0,
                            -1);
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

    ServiceComputation<JacsServiceResult<List<ContentStack>>> copyContent(JacsServiceData jacsServiceData, String storageURL, String storagePath, List<ContentStack> contentList) {
        return computationFactory.<JacsServiceResult<List<ContentStack>>>newComputation()
                .supply(() -> new JacsServiceResult<>(
                        jacsServiceData,
                        contentList.stream()
                                .peek(contentEntry -> {
                                    Stream.of(contentEntry.getMainRep())
                                            .filter(sci -> sci.getRemoteInfo().getEntryURL() != null)
                                            .filter(sci -> sci.getRemoteInfo().isNotCollection())
                                            .forEach(sci -> copyContent(sci, storageURL, storagePath, jacsServiceData.getOwnerKey(), ResourceHelper.getAuthToken(jacsServiceData.getResources())));
                                })
                                .collect(Collectors.toList())
                ));
    }

    private void copyContent(StorageContentInfo storageContentInfo, String storageURL, String storagePathParam, String subjectKey, String authToken) {
        InputStream inputStream = storageService.getStorageContent(
                storageContentInfo.getRemoteInfo().getEntryURL(),
                subjectKey,
                authToken
        );
        try {
            if (inputStream != null) {
                logger.info("Copy {} to {} {}", storageContentInfo, storageURL, storagePathParam);
                String storagePath = StringUtils.appendIfMissing(storagePathParam, "/");
                storageContentInfo.setRemoteInfo(storageService.putStorageContent(
                        storageURL,
                        storagePath + storageContentInfo.getRemoteInfo().getEntryRelativePath(),
                        subjectKey,
                        authToken,
                        inputStream
                ));
            }
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

    private void uploadContent(StorageContentInfo storageContentInfo, String storageURL, String subjectKey, String authToken) {
        Path localPath = Paths.get(storageContentInfo.getLocalBasePath()).resolve(storageContentInfo.getLocalRelativePath());
        InputStream inputStream = openLocalContent(localPath);
        try {
            if (inputStream != null) {
                logger.info("Upload {}({}) to {}", storageContentInfo, localPath, storageURL);
                storageContentInfo.setRemoteInfo(storageService.putStorageContent(
                        storageURL,
                        storageContentInfo.getRemoteInfo().getEntryRelativePath(),
                        subjectKey,
                        authToken,
                        inputStream
                ));
            }
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

    private InputStream openLocalContent(Path localPath) {
        if (Files.exists(localPath)) {
            try {
                return new FileInputStream(localPath.toFile());
            } catch (FileNotFoundException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            return null;
        }
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
                            addContentRepresentation(inputStack, localMIPSRootPath.toString(), localMIPSRootPath.relativize(Paths.get(mipsFile)).toString(), "mips");
                        });
            }
        });
        return contentList;
    }

    void addContentRepresentation(ContentStack contentStack, String localBasePath, String localRelativePath, String remotePathPrefixParam) {
        StorageContentInfo newContentRepInfo = new StorageContentInfo();
        newContentRepInfo.setLocalBasePath(localBasePath);
        newContentRepInfo.setLocalRelativePath(localRelativePath);
        StorageContentInfo mainRep = contentStack.getMainRep();
        if (mainRep.getRemoteInfo() != null) {
            String remotePathPrefix;
            if (StringUtils.isNotBlank(mainRep.getRemoteInfo().getEntryRelativePath())) {
                Path remoteEntryPath;
                if (mainRep.getRemoteInfo().isCollection()) {
                    remoteEntryPath = Paths.get(mainRep.getRemoteInfo().getEntryRelativePath());
                } else {
                    // for files take the file's parent folder
                    Path remoteEntryParentPath = Paths.get(mainRep.getRemoteInfo().getEntryRelativePath()).getParent();
                    if (remoteEntryParentPath == null) {
                        remoteEntryPath = Paths.get("");
                    } else {
                        remoteEntryPath = remoteEntryParentPath;
                    }
                }
                if (StringUtils.isBlank(remotePathPrefixParam)) {
                            Paths.get(mainRep.getRemoteInfo().getEntryRelativePath());
                    remotePathPrefix = remoteEntryPath.toString();
                } else {
                    remotePathPrefix = remoteEntryPath.resolve(remotePathPrefixParam).toString();
                }
            } else {
                remotePathPrefix = remotePathPrefixParam;
            }
            // if the main representation has remote info copy it
            newContentRepInfo.setRemoteInfo(new StorageEntryInfo(
                    mainRep.getRemoteInfo().getStorageId(), // I want the MIPs in the same bundle if one exists
                    mainRep.getRemoteInfo().getStorageURL(),
                    null, // I don't know the entry URL yet
                    mainRep.getRemoteInfo().getStorageRootLocation(),
                    mainRep.getRemoteInfo().getStorageRootPathURI(),
                    constructStorageEntryPath(newContentRepInfo, remotePathPrefix),
                    false
            ));
        }
        contentStack.addRepresentation(newContentRepInfo);
    }

    String constructStorageEntryPath(StorageContentInfo storageContentInfo, String pathPrefix) {
        if (StringUtils.isBlank(pathPrefix)) {
            return storageContentInfo.getLocalRelativePath();
        } else {
            return Paths.get(pathPrefix, storageContentInfo.getLocalRelativePath()).toString();
        }
    }

}
