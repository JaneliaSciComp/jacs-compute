package org.janelia.jacs2.asyncservice.dataimport;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.dataservice.storage.StoragePathURI;
import org.janelia.jacs2.dataservice.workspace.FolderService;
import org.janelia.model.domain.DomainUtils;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.sample.Image;
import org.janelia.model.domain.workspace.TreeNode;
import org.janelia.model.service.JacsServiceData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
                                                 String dataNodeName,
                                                 Number parentDataNodeId,
                                                 String parentWorkspaceOwnerKey,
                                                 FileType defaultFileType,
                                                 String ownerKey) {
        if (StringUtils.isNotBlank(dataNodeName)) {
            TreeNode dataFolder = folderService.getOrCreateFolder(parentDataNodeId, parentWorkspaceOwnerKey, dataNodeName, ownerKey);
            return contentList.stream()
                    .filter(contentEntry -> contentEntry.getMainRep().getRemoteInfo().isNotCollection()) // only upload files
                    .peek(contentEntry -> {
                        LOG.info("Add {} to {}", contentEntry, dataFolder);
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
                            DomainUtils.setFilepath(imageStack, defaultFileType, mainRepPathURI.toString());
                        } else {
                            mainRepFileTypes.forEach(ft -> DomainUtils.setFilepath(imageStack, ft, mainRepPathURI.toString()));
                        }
                        contentEntry.getAdditionalReps().forEach(ci -> {
                            StoragePathURI ciStoragePathURI = ci.getRemoteInfo().getEntryPathURI()
                                    .orElseGet(() -> new StoragePathURI(ci.getRemoteInfo().getEntryRelativePath()));
                            Set<FileType> fileTypes = FileTypeHelper.getFileTypeByExtension(ciStoragePathURI.getStoragePath());
                            if (fileTypes.isEmpty()) {
                                DomainUtils.setFilepath(imageStack, defaultFileType, ciStoragePathURI.toString());
                            } else {
                                fileTypes.forEach(ft -> DomainUtils.setFilepath(imageStack, ft, ciStoragePathURI.toString()));
                            }
                        });
                        folderService.addImageStack(dataFolder, imageStack, ownerKey);
                        contentEntry.setDataNodeId(dataFolder.getId());
                    })
                    .collect(Collectors.toList());
        } else {
            return contentList;
        }
    }

    List<ContentStack> addStandaloneContentToTreeNode(List<ContentStack> contentList,
                                                      String dataNodeName,
                                                      Number parentDataNodeId,
                                                      String parentWorkspaceOwnerKey,
                                                      FileType defaultFileType,
                                                      String ownerKey) {
        if (StringUtils.isNotBlank(dataNodeName)) {
            TreeNode dataFolder = folderService.getOrCreateFolder(parentDataNodeId, parentWorkspaceOwnerKey, dataNodeName, ownerKey);
            return contentList.stream()
                    .filter(contentEntry -> contentEntry.getMainRep().getRemoteInfo().isNotCollection())
                    .peek(contentEntry -> {
                        Stream.concat(Stream.of(contentEntry.getMainRep()), contentEntry.getAdditionalReps().stream())
                                .forEach(ci -> {
                                    LOG.info("Add {} to {}", ci.getRemoteInfo(), dataFolder);
                                    StoragePathURI storagePathURI = ci.getRemoteInfo().getEntryPathURI()
                                            .orElseGet(() -> new StoragePathURI(ci.getRemoteInfo().getEntryRelativePath()));
                                    Set<FileType> fileTypes = FileTypeHelper.getFileTypeByExtension(storagePathURI.getStoragePath());
                                    if (fileTypes.isEmpty()) {
                                        folderService.addImageFile(dataFolder,
                                                FileUtils.getFileName(storagePathURI.getStoragePath()),
                                                storagePathURI.getParent().map(spURI -> spURI.toString()).orElse(""),
                                                storagePathURI.toString(),
                                                defaultFileType,
                                                true,
                                                ownerKey
                                        );
                                    } else {
                                        fileTypes.forEach(ft -> folderService.addImageFile(dataFolder,
                                                FileUtils.getFileName(storagePathURI.getStoragePath()),
                                                storagePathURI.getParent().map(spURI -> spURI.toString()).orElse(""),
                                                storagePathURI.toString(),
                                                ft,
                                                true,
                                                ownerKey
                                        ));
                                    }
                                });
                        contentEntry.setDataNodeId(dataFolder.getId());
                    })
                    .collect(Collectors.toList());
        } else {
            return contentList;
        }
    }

}
