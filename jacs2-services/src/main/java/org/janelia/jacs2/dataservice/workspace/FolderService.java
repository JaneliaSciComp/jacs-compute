package org.janelia.jacs2.dataservice.workspace;

import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.StringUtils;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.TreeNodeDao;
import org.janelia.model.access.domain.dao.WorkspaceNodeDao;
import org.janelia.model.domain.DomainUtils;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.sample.Image;
import org.janelia.model.domain.workspace.TreeNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

public class FolderService {

    private static final Logger LOG = LoggerFactory.getLogger(FolderService.class);

    private final WorkspaceNodeDao workspaceNodeDao;
    private final TreeNodeDao folderDao;
    private final LegacyDomainDao legacyDomainDao;

    @Inject
    public FolderService(WorkspaceNodeDao workspaceNodeDao,
                         TreeNodeDao folderDao,
                         LegacyDomainDao legacyDomainDao) {
        this.workspaceNodeDao = workspaceNodeDao;
        this.folderDao = folderDao;
        this.legacyDomainDao = legacyDomainDao;
    }

    /**
     * Create
     * @param parentFolderId
     * @param parentWorkspaceOwnerKey
     * @param folderName
     * @param subjectKey
     * @return
     */
    public TreeNode getOrCreateFolder(Number parentFolderId, String parentWorkspaceOwnerKey, String folderName, String subjectKey) {
        try {
            TreeNode parentFolder;
            if (parentFolderId == null && StringUtils.isBlank(parentWorkspaceOwnerKey)) {
                parentFolder = workspaceNodeDao.getDefaultWorkspace(subjectKey);
                if (parentFolder == null) {
                    // if no default workspace exists for subjectKey, just create it
                    parentFolder = workspaceNodeDao.createDefaultWorkspace(subjectKey);
                }
            } else {
                if (parentFolderId != null) {
                    parentFolder = folderDao.findEntityByIdReadableBySubjectKey(parentFolderId.longValue(), subjectKey);
                    if (parentFolder == null)
                        LOG.warn("No folder with id:{} readable by {}", parentFolderId, subjectKey);
                } else {
                    parentFolder = workspaceNodeDao.getDefaultWorkspace(parentWorkspaceOwnerKey);
                    if (parentFolder == null)
                        LOG.warn("No default workspace found for {}", parentWorkspaceOwnerKey);
                }
            }
            if (parentFolder == null) {
                throw new IllegalArgumentException("No parent folder" +
                        (parentFolderId != null ? " found for " + parentFolderId : " created"));
            }
            List<String> folderPathComponents = Splitter.on('/').trimResults().omitEmptyStrings().splitToList(folderName);
            TreeNode newFolder = null;
            for (String folderComponent : folderPathComponents) {
                List<TreeNode> existingFoldersWithSameName = folderDao.getNodesByParentNameAndOwnerKey(parentFolder.getId(), folderComponent, subjectKey);
                if (existingFoldersWithSameName.isEmpty()) {
                    TreeNode folder =  new TreeNode();
                    folder.setName(folderComponent);
                    newFolder = folderDao.saveBySubjectKey(folder, subjectKey);
                    legacyDomainDao.addChildren(subjectKey, parentFolder, ImmutableList.of(Reference.createFor(newFolder)));
                } else {
                    // pick the first from the list
                    newFolder = existingFoldersWithSameName.get(0); // no need to add this as a child for the current parent because that's how we got it
                    parentFolder = newFolder; // in case this is not the last component
                }
                parentFolder = newFolder;
            }
            return newFolder;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public void addImageStack(TreeNode folder, Image imageStack, String subjectKey) {
        try {
            LOG.info("Add image stack {} to {} for {}", imageStack, folder, subjectKey);
            legacyDomainDao.save(subjectKey, imageStack);
            legacyDomainDao.addChildren(subjectKey, folder, ImmutableList.of(Reference.createFor(imageStack)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public void addImageFile(TreeNode folder, String imageName, String imageFolderPath, String imageFilePath, FileType imageFileType, boolean userDataFlag, String subjectKey) {
        try {
            Image imageFile = new Image();
            imageFile.setName(imageName);
            imageFile.setUserDataFlag(userDataFlag);
            imageFile.setFilepath(imageFolderPath);
            DomainUtils.setFilepath(imageFile, imageFileType, imageFilePath);
            legacyDomainDao.save(subjectKey, imageFile);
            legacyDomainDao.addChildren(subjectKey, folder, ImmutableList.of(Reference.createFor(imageFile)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
