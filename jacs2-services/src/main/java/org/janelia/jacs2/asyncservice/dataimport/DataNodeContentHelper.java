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

import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This is a helper class for creating the corresponding TreeNode entries for the provided storage content.
 */
class DataNodeContentHelper {

    private final ServiceComputationFactory computationFactory;
    private final FolderService folderService;
    private final Logger logger;

    DataNodeContentHelper(ServiceComputationFactory computationFactory,
                          FolderService folderService,
                          Logger logger) {
        this.computationFactory = computationFactory;
        this.folderService = folderService;
        this.logger = logger;
    }

    ServiceComputation<JacsServiceResult<List<ContentStack>>> addContentStackToTreeNode(JacsServiceData jacsServiceData,
                                                                                        String dataNodeName,
                                                                                        Number parentDataNodeId,
                                                                                        FileType defaultFileType,
                                                                                        List<ContentStack> contentList) {
        if (StringUtils.isNotBlank(dataNodeName)) {
            TreeNode dataFolder = folderService.getOrCreateFolder(parentDataNodeId, dataNodeName, jacsServiceData.getOwnerKey());
            return computationFactory.<JacsServiceResult<List<ContentStack>>>newComputation()
                    .supply(() -> new JacsServiceResult<>(jacsServiceData, contentList.stream()
                            .filter(contentEntry -> contentEntry.getMainRep().getRemoteInfo().isNotCollection())
                            .peek(contentEntry -> {
                                logger.info("Add {} to {}", contentEntry, dataFolder);
                                Image imageStack = new Image();
                                String entryRelativePath = contentEntry.getMainRep().getRemoteInfo().getEntryRelativePath();
                                String imageName = Paths.get(entryRelativePath).getFileName().toString();
                                imageStack.setName(imageName);
                                imageStack.setUserDataFlag(true);
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
                                folderService.addImageStack(dataFolder,
                                        imageStack,
                                        jacsServiceData.getOwnerKey());
                                contentEntry.setDataNodeId(dataFolder.getId());
                            })
                            .collect(Collectors.toList())));
        } else {
            return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData, contentList));
        }
    }

    ServiceComputation<JacsServiceResult<List<ContentStack>>> addStandaloneContentToTreeNode(JacsServiceData jacsServiceData,
                                                                                             String dataNodeName,
                                                                                             Number parentDataNodeId,
                                                                                             FileType defaultFileType,
                                                                                             List<ContentStack> contentList) {
        if (StringUtils.isNotBlank(dataNodeName)) {
            TreeNode dataFolder = folderService.getOrCreateFolder(parentDataNodeId, dataNodeName, jacsServiceData.getOwnerKey());
            return computationFactory.<JacsServiceResult<List<ContentStack>>>newComputation()
                    .supply(() -> new JacsServiceResult<>(jacsServiceData, contentList.stream()
                            .filter(contentEntry -> contentEntry.getMainRep().getRemoteInfo().isNotCollection())
                            .peek(contentEntry -> {
                                Stream.concat(Stream.of(contentEntry.getMainRep()), contentEntry.getAdditionalReps().stream())
                                        .forEach(ci -> {
                                            logger.info("Add {} to {}", ci.getRemoteInfo(), dataFolder);
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
                                                        jacsServiceData.getOwnerKey()
                                                );
                                            } else {
                                                fileTypes.forEach(ft -> folderService.addImageFile(dataFolder,
                                                        FileUtils.getFileName(storagePathURI.getStoragePath()),
                                                        storagePathURI.getParent().map(spURI -> spURI.toString()).orElse(""),
                                                        storagePathURI.toString(),
                                                        ft,
                                                        true,
                                                        jacsServiceData.getOwnerKey()
                                                ));
                                            }
                                        });
                                contentEntry.setDataNodeId(dataFolder.getId());
                            })
                            .collect(Collectors.toList())));
        } else {
            return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData, contentList));
        }
    }

}
