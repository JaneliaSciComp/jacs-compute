package org.janelia.jacs2.asyncservice.dataimport;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.dataservice.workspace.FolderService;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.workspace.TreeNode;
import org.janelia.model.service.JacsServiceData;
import org.slf4j.Logger;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This is a helper class for creating the corresponding TreeNode entries for the provided storage content.
 */
class DataNodeContentHelper {

    private static final Set<String> LOSSLESS_IMAGE_EXTENSIONS = ImmutableSet.of(
            ".lsm", ".tif", ".raw", ".v3draw", ".vaa3draw", ".v3dpbd"
    );

    private static final Set<String> UNCLASSIFIED_2D_EXTENSIONS = ImmutableSet.of(
            ".png", ".jpg", ".tif", ".img", ".gif"
    );

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

    ServiceComputation<JacsServiceResult<List<StorageContentInfo>>> addContentToTreeNode(JacsServiceData jacsServiceData,
                                                                                         String dataNodeName,
                                                                                         Number parentDataNodeId,
                                                                                         FileType defaultFileType,
                                                                                         List<StorageContentInfo> contentList) {
        if (StringUtils.isNotBlank(dataNodeName)) {
            TreeNode dataFolder = folderService.getOrCreateFolder(parentDataNodeId, dataNodeName, jacsServiceData.getOwnerKey());
            return computationFactory.<JacsServiceResult<List<StorageContentInfo>>>newComputation()
                    .supply(() -> new JacsServiceResult<>(jacsServiceData, contentList.stream()
                            .peek(contentEntry -> {
                                logger.info("Add {} to {}", contentEntry.getRemoteInfo().getEntryPath(), dataFolder);
                                FileType fileType = contentEntry.getFileType();
                                if (fileType == null) {
                                    fileType = getFileTypeByExtension(contentEntry.getRemoteInfo().getEntryPath(), defaultFileType);
                                }
                                folderService.addImageFile(dataFolder,
                                        contentEntry.getRemoteInfo().getEntryPath(),
                                        fileType,
                                        jacsServiceData.getOwnerKey());
                                contentEntry.setDataNodeId(dataFolder.getId());
                            })
                            .collect(Collectors.toList())));
        } else {
            return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData, contentList));
        }
    }

    FileType getFileTypeByExtension(String fileArtifact, FileType defaultFileType) {
        String fileArtifactExt = FileUtils.getFileExtensionOnly(fileArtifact);
        if (StringUtils.isNotBlank(fileArtifactExt)) {
            if (LOSSLESS_IMAGE_EXTENSIONS.contains(fileArtifactExt.toLowerCase())) {
                return FileType.LosslessStack;
            } else if (UNCLASSIFIED_2D_EXTENSIONS.contains(fileArtifactExt.toLowerCase())) {
                return FileType.Unclassified2d;
            } else {
                return defaultFileType;
            }
        } else {
            return defaultFileType;
        }
    }

}
