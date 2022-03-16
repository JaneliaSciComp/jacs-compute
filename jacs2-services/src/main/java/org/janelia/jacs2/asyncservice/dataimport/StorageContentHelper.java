package org.janelia.jacs2.asyncservice.dataimport;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.dataservice.storage.StorageEntryInfo;
import org.janelia.jacs2.dataservice.storage.StoragePathURI;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Helper class for downloading/uploading content to JADE.
 */
public class StorageContentHelper {

    private static final Logger LOG = LoggerFactory.getLogger(StorageContentHelper.class);

    protected final StorageService storageService;

    public StorageContentHelper(StorageService storageService) {
        this.storageService = storageService;
    }

    public Optional<StorageEntryInfo> lookupStorage(String storagePath, String ownerKey, String authToken) {
        LOG.info("Lookup storage for {}", storagePath);
        return storageService.findStorageVolumes(storagePath, ownerKey, authToken)
                .stream().findFirst()
                .map(jadeStorageVolume -> {
                    String relativeStoragePath;
                    if (StringUtils.startsWith(storagePath, StringUtils.appendIfMissing(jadeStorageVolume.getStorageVirtualPath(), "/"))) {
                        relativeStoragePath = Paths.get(jadeStorageVolume.getStorageVirtualPath()).relativize(Paths.get(storagePath)).toString();
                    } else if (StringUtils.startsWith(storagePath, StringUtils.appendIfMissing(jadeStorageVolume.getBaseStorageRootDir(), "/"))) {
                        relativeStoragePath = Paths.get(jadeStorageVolume.getBaseStorageRootDir()).relativize(Paths.get(storagePath)).toString();
                    } else {
                        relativeStoragePath = "";
                    }
                    LOG.info("Found {} for {}; the new path relative to the volume's root is {}", jadeStorageVolume, storagePath, relativeStoragePath);
                    return new StorageEntryInfo(
                            jadeStorageVolume.getId(),
                            jadeStorageVolume.getVolumeStorageURI(),
                            storageService.getEntryURI(jadeStorageVolume.getVolumeStorageURI(), relativeStoragePath),
                            jadeStorageVolume.getStorageVirtualPath(),
                            new StoragePathURI(relativeStoragePath),
                            relativeStoragePath,
                            null, // size is not known
                            true, // this really does not matter but assume the path is a directory
                            null // unspecified mime
                    );
                });
    }

    public List<ContentStack> listContent(String storageURL, String storagePath, String ownerKey, String authToken) {
        return listContent(storageURL, storagePath, -1, ownerKey, authToken);
    }

    public List<ContentStack> listContent(String storageURL, String storagePath, int depth, String ownerKey, String authToken) {
        LOG.debug("List content of {} from {}", storagePath, storageURL);
        return storageService.listStorageContent(storageURL, storagePath, ownerKey, authToken, depth, 0, -1).stream()
                .map(entry -> {
                    StorageContentInfo storageContentInfo = new StorageContentInfo();
                    storageContentInfo.setRemoteInfo(entry);
                    storageContentInfo.setSize(entry.getSize());
                    return new ContentStack(storageContentInfo);
                })
                .collect(Collectors.toList());
    }

    /**
     * Download content from the contentList to the given downloadLocation only if it cannot be reached.
     *
     * @param contentList
     * @param downloadLocation
     * @param ownerKey
     * @param authToken
     * @return
     */
    List<ContentStack> downloadUnreachableContent(List<ContentStack> contentList, Path downloadLocation, String ownerKey, String authToken) {
        return contentList.stream()
                .peek(contentEntry -> {
                    try {
                        Path remoteEntryPath = Paths.get(contentEntry.getMainRep().getRemoteFullPath());
                        if (Files.notExists(remoteEntryPath)) {
                            contentEntry.getMainRep().setLocallyReachable(false);
                            // if the content is not accessible using the storageRoot then try to download it to the download location (if it's not already there)
                            String entryRelativePathName = contentEntry.getMainRep().getRemoteInfo().getEntryRelativePath();
                            Path entryRelativePath = Paths.get(sanitizeFileName(entryRelativePathName));
                            Path entryFullPath = downloadLocation.resolve(entryRelativePath);
                            if (Files.notExists(entryFullPath) || Files.size(entryFullPath) == 0) {
                                // no local copy found - so download it
                                Files.createDirectories(entryFullPath.getParent());
                                Files.copy(
                                        storageService.getStorageContent(contentEntry.getMainRep().getRemoteInfo().getEntryURL(), ownerKey, authToken),
                                        entryFullPath,
                                        StandardCopyOption.REPLACE_EXISTING);
                            }
                            // set local path info
                            contentEntry.getMainRep().setLocalBasePath(downloadLocation.toString());
                            contentEntry.getMainRep().setLocalRelativePath(entryRelativePath.toString());
                            contentEntry.getMainRep().setSize(Files.size(entryFullPath));
                        } else {
                            contentEntry.getMainRep().setLocallyReachable(true);
                            contentEntry.getMainRep().setSize(Files.size(remoteEntryPath));
                        }
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .collect(Collectors.toList());
    }

    private String sanitizeFileName(String fname) {
        if (StringUtils.isBlank(fname)) {
            return fname;
        } else {
            return fname.replace('#', '_');
        }
    }

    void removeRemoteContent(String storageURL, String storagePath, String ownerKey, String authToken) {
        storageService.removeStorageContent(storageURL, storagePath, ownerKey, authToken);
    }

    List<ContentStack> removeLocalContent(List<ContentStack> contentList) {
        return contentList.stream()
                .peek((ContentStack contentEntry) -> {
                    Stream.concat(Stream.of(contentEntry.getMainRep()), contentEntry.getAdditionalReps().stream())
                            .forEach(sci -> {
                                if (sci.getLocalRelativePath() != null) {
                                    Path localContentPath = Paths.get(sci.getLocalFullPath());
                                    LOG.info("Clean local file {} ", localContentPath);
                                    try {
                                        FileUtils.deletePath(localContentPath);
                                    } catch (IOException e) {
                                        LOG.warn("Error deleting {}", localContentPath, e);
                                    }
                                }
                            });
                })
                .collect(Collectors.toList());
    }

    /**
     * Copy contentList to the location identified by storageURL and storagePath.
     *
     * @param contentList
     * @param storageURL
     * @param storagePath
     * @param ownerKey
     * @param authToken
     * @return
     */
    List<ContentStack> copyContent(List<ContentStack> contentList, String storageURL, String storagePath, String ownerKey, String authToken) {
        return contentList
                .stream()
                .peek(contentEntry -> {
                    StorageContentInfo sci = contentEntry.getMainRep();
                    // if entryURL is there it means that we already have the content on the specified storage
                    // we transfer it only if the content is a file and is not already on the storage
                    if (sci.getRemoteInfo().isNotCollection() && sci.getRemoteInfo().getEntryURL() != null) {
                        copyContent(sci, storageURL, storagePath, ownerKey, authToken);
                    }
                })
                .collect(Collectors.toList());
    }

    private void copyContent(StorageContentInfo storageContentInfo, String storageURL, String storagePathParam, String ownerKey, String authToken) {
        InputStream inputStream = storageService.getStorageContent(storageContentInfo.getRemoteInfo().getEntryURL(), ownerKey, authToken);
        try {
            if (inputStream != null) {
                LOG.info("Copy {} to {} {}", storageContentInfo, storageURL, storagePathParam);
                String storagePath = StringUtils.appendIfMissing(storagePathParam, "/");
                storageContentInfo.setRemoteInfo(storageService.putStorageContent(
                        storageURL,
                        storagePath + storageContentInfo.getRemoteInfo().getEntryRelativePath(),
                        ownerKey,
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

    /**
     * Upload contentList to the URL location.
     *
     * @param contentList
     * @param storageURL
     * @param ownerKey
     * @param authToken
     * @return
     */
    List<ContentStack> uploadContent(List<ContentStack> contentList, String storageURL, String ownerKey, String authToken) {
        LOG.info("Upload content stack {} to {} for {}", contentList, storageURL, ownerKey);
        return contentList.stream()
                .peek(contentEntry -> {
                    Stream.concat(Stream.of(contentEntry.getMainRep()), contentEntry.getAdditionalReps().stream())
                            .filter(sci -> sci.getRemoteInfo().getEntryURL() == null) // only upload the ones that don't have a storageEntryURL, i.e., not there yet
                            .forEach(sci -> uploadContent(sci, storageURL, ownerKey, authToken));
                })
                .collect(Collectors.toList())
                ;
    }

    private void uploadContent(StorageContentInfo storageContentInfo, String storageURL, String subjectKey, String authToken) {
        Path localPath = Paths.get(storageContentInfo.getLocalBasePath()).resolve(storageContentInfo.getLocalRelativePath());
        InputStream inputStream = openLocalContent(localPath);
        try {
            if (inputStream != null) {
                LOG.info("Upload {}({}) to {} for {}", storageContentInfo, localPath, storageURL, subjectKey);
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

    void addContentRepresentation(ContentStack contentStack, String localBasePath, String localRelativePath, Long size, String remotePathPrefixParam) {
        StorageContentInfo newContentRepInfo = new StorageContentInfo();
        newContentRepInfo.setLocalBasePath(localBasePath);
        newContentRepInfo.setLocalRelativePath(localRelativePath);
        newContentRepInfo.setSize(size);
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
                    size,
                    false,
                    null // unspecified mimetype
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
