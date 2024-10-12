package org.janelia.jacs2.asyncservice.dataimport;

import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.dataservice.workspace.FolderService;
import org.janelia.jacsstorage.clients.api.StorageEntryInfo;
import org.janelia.jacsstorage.clients.api.StoragePathURI;
import org.janelia.model.domain.DomainUtils;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.sample.Image;
import org.janelia.model.domain.workspace.TreeNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a helper class for creating the corresponding TreeNode entries for the provided storage content.
 */
class DataNodeContentHelper {
    private static final Logger LOG = LoggerFactory.getLogger(DataNodeContentHelper.class);

    private final FolderService folderService;

    DataNodeContentHelper(FolderService folderService) {
        this.folderService = folderService;
    }

    List<ContentStack> addContentStackToTreeNode(List<ContentStack> contentList,
                                                 Number parentDataNodeId,
                                                 String parentWorkspaceOwnerKey,
                                                 String dataNodeName,
                                                 String relativizeToDataLocation,
                                                 FileType defaultFileType,
                                                 String ownerKey) {
        if (StringUtils.isNotBlank(dataNodeName)) {
            LOG.info("Add {} to node {}/{} for {}", contentList, parentDataNodeId, dataNodeName, ownerKey);
            TreeNode dataFolder = folderService.getOrCreateFolder(parentDataNodeId, parentWorkspaceOwnerKey, dataNodeName, ownerKey);
            return contentList.stream()
                    .filter(contentEntry -> contentEntry.getMainRep().getRemoteInfo().isNotCollection()) // only upload files
                    .peek(contentEntry -> {
                        TreeNode entryFolder;
                        if (StringUtils.isNotBlank(relativizeToDataLocation)) {
                            String entryFolderName = getEntryFolderName(contentEntry.getMainRep().getRemoteInfo(), relativizeToDataLocation);
                            if (StringUtils.isNotBlank(entryFolderName)) {
                                entryFolder = folderService.getOrCreateFolder(dataFolder.getId(), null, entryFolderName, ownerKey);
                            } else {
                                entryFolder = dataFolder;
                            }
                        } else {
                            entryFolder = dataFolder;
                        }
                        LOG.info("Add {} to {}", contentEntry, entryFolder);
                        Image imageStack = new Image();
                        String entryRelativePath = contentEntry.getMainRep().getRemoteInfo().getEntryRelativePath();
                        String imageName = Paths.get(entryRelativePath).getFileName().toString();
                        imageStack.setName(imageName);
                        imageStack.setUserDataFlag(true);
                        imageStack.setFileSize(contentEntry.getMainRep().getSize());
                        StoragePathURI mainRepPathURI = contentEntry.getMainRep().getRemoteInfo().getEntryPathURI()
                                .orElseGet(() -> new StoragePathURI(contentEntry.getMainRep().getRemoteInfo().getEntryRelativePath()));
                        imageStack.setFilepath(mainRepPathURI.getParent().map(spURI -> spURI.toString()).orElse(""));
                        Set<FileType> mainRepFileTypes = FileTypeHelper.getFileTypeByExtension(mainRepPathURI.getStoragePath());
                        if (mainRepFileTypes.isEmpty()) {
                            LOG.info("Add main file {} of default type ({}) to {}", mainRepPathURI, defaultFileType, imageStack);
                            DomainUtils.setFilepath(imageStack, defaultFileType, mainRepPathURI.toString());
                        } else {
                            mainRepFileTypes.forEach(ft -> {
                                LOG.info("Add main file {} of type {} to {}", mainRepPathURI, ft, imageStack);
                                DomainUtils.setFilepath(imageStack, ft, mainRepPathURI.toString());
                            });
                        }
                        contentEntry.getAdditionalReps().forEach(ci -> {
                            StoragePathURI ciStoragePathURI = ci.getRemoteInfo().getEntryPathURI()
                                    .orElseGet(() -> new StoragePathURI(ci.getRemoteInfo().getEntryRelativePath()));
                            Set<FileType> fileTypes = FileTypeHelper.getFileTypeByExtension(ciStoragePathURI.getStoragePath());
                            if (fileTypes.isEmpty()) {
                                LOG.info("Add file {} of default type ({}) to {}", ciStoragePathURI, defaultFileType, imageStack);
                                DomainUtils.setFilepath(imageStack, defaultFileType, ciStoragePathURI.toString());
                            } else {
                                fileTypes.forEach(ft -> {
                                    LOG.info("Add file {} of type {} to {}", ciStoragePathURI, ft, imageStack);
                                    DomainUtils.setFilepath(imageStack, ft, ciStoragePathURI.toString());
                                });
                            }
                        });
                        folderService.addImageStack(entryFolder, imageStack, ownerKey);
                        contentEntry.setDataNodeId(entryFolder.getId());
                    })
                    .collect(Collectors.toList());
        } else {
            return contentList;
        }
    }

    List<ContentStack> addStandaloneContentToTreeNode(List<ContentStack> contentList,
                                                      Number parentDataNodeId,
                                                      String parentWorkspaceOwnerKey,
                                                      String dataNodeName,
                                                      String relativizeToDataLocation,
                                                      FileType defaultFileType,
                                                      String ownerKey) {
        if (StringUtils.isNotBlank(dataNodeName)) {
            LOG.info("Add {} to node {}/{} for {}", contentList, parentDataNodeId, dataNodeName, ownerKey);
            TreeNode dataFolder = folderService.getOrCreateFolder(parentDataNodeId, parentWorkspaceOwnerKey, dataNodeName, ownerKey);
            return contentList.stream()
                    .filter(contentEntry -> contentEntry.getMainRep().getRemoteInfo().isNotCollection())
                    .peek(contentEntry -> {
                        TreeNode entryFolder;
                        if (StringUtils.isNotBlank(relativizeToDataLocation)) {
                            String entryFolderName = getEntryFolderName(contentEntry.getMainRep().getRemoteInfo(), relativizeToDataLocation);
                            if (StringUtils.isNotBlank(entryFolderName)) {
                                entryFolder = folderService.getOrCreateFolder(dataFolder.getId(), null, entryFolderName, ownerKey);
                            } else {
                                entryFolder = dataFolder;
                            }
                        } else {
                            entryFolder = dataFolder;
                        }
                        Stream.concat(Stream.of(contentEntry.getMainRep()), contentEntry.getAdditionalReps().stream())
                                .forEach(ci -> {
                                    LOG.info("Add {} to {}", ci.getRemoteInfo(), entryFolder);
                                    StoragePathURI storagePathURI = ci.getRemoteInfo().getEntryPathURI()
                                            .orElseGet(() -> new StoragePathURI(ci.getRemoteInfo().getEntryRelativePath()));
                                    Set<FileType> fileTypes = FileTypeHelper.getFileTypeByExtension(storagePathURI.getStoragePath());
                                    if (fileTypes.isEmpty()) {
                                        folderService.addImageFile(entryFolder,
                                                FileUtils.getFileName(storagePathURI.getStoragePath()),
                                                storagePathURI.getParent().map(spURI -> spURI.toString()).orElse(""),
                                                storagePathURI.toString(),
                                                defaultFileType,
                                                true,
                                                ownerKey
                                        );
                                    } else {
                                        fileTypes.forEach(ft -> folderService.addImageFile(entryFolder,
                                                FileUtils.getFileName(storagePathURI.getStoragePath()),
                                                storagePathURI.getParent().map(spURI -> spURI.toString()).orElse(""),
                                                storagePathURI.toString(),
                                                ft,
                                                true,
                                                ownerKey
                                        ));
                                    }
                                });
                        contentEntry.setDataNodeId(entryFolder.getId());
                    })
                    .collect(Collectors.toList());
        } else {
            return contentList;
        }
    }

    private String getEntryFolderName(StorageEntryInfo storageEntryInfo, String relativizeToDataLocation) {
        if (StringUtils.isBlank(relativizeToDataLocation)) {
            return "";
        } else {
            String entryURL = storageEntryInfo.getEntryURL();
            if (StringUtils.startsWith(entryURL, relativizeToDataLocation)) {
                String entryNameRelativeToDataLocation = StringUtils.removeStart(
                        StringUtils.replace(entryURL, relativizeToDataLocation, ""),
                        "/"
                );
                int separatorIndex = entryNameRelativeToDataLocation.lastIndexOf('/');
                if (separatorIndex == -1) {
                    return "";
                } else {
                    return entryNameRelativeToDataLocation.substring(0, separatorIndex);
                }
            } else {
                return ""; // don't put it in any subfolders
            }
        }
    }
}
