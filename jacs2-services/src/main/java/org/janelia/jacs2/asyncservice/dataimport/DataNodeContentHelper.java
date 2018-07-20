package org.janelia.jacs2.asyncservice.dataimport;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.dataservice.workspace.FolderService;
import org.janelia.model.access.domain.DomainUtils;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.sample.Image;
import org.janelia.model.domain.workspace.TreeNode;
import org.janelia.model.service.JacsServiceData;
import org.slf4j.Logger;

import java.util.List;
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
                                imageStack.setFilepath(contentEntry.getMainRep().getRemoteInfo().getEntryPath());
                                contentEntry.getAdditionalReps().forEach(ci -> {
                                    FileType fileType = ci.getFileType();
                                    if (fileType == null) {
                                        fileType = FileTypeHelper.getFileTypeByExtension(ci.getRemoteInfo().getEntryPath(), defaultFileType);
                                    }
                                    DomainUtils.setFilepath(imageStack, fileType, ci.getRemoteInfo().getEntryPath());
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
                                            logger.info("Add {} to {}", ci.getRemoteInfo().getEntryPath(), dataFolder);
                                            FileType fileType = ci.getFileType();
                                            if (fileType == null) {
                                                fileType = FileTypeHelper.getFileTypeByExtension(ci.getRemoteInfo().getEntryPath(), defaultFileType);
                                            }
                                            folderService.addImageFile(dataFolder,
                                                    ci.getRemoteInfo().getEntryPath(),
                                                    fileType,
                                                    jacsServiceData.getOwnerKey()
                                            );
                                        });
                                contentEntry.setDataNodeId(dataFolder.getId());
                            })
                            .collect(Collectors.toList())));
        } else {
            return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData, contentList));
        }
    }

}
