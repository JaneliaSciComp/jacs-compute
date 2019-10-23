package org.janelia.jacs2.dataservice.workspace;

import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.StringUtils;
import org.janelia.model.access.dao.LegacyDomainDao;
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

    private final LegacyDomainDao folderDao;

    @Inject
    public FolderService(LegacyDomainDao folderDao) {
        this.folderDao = folderDao;
    }

    public TreeNode getOrCreateFolder(Number parentFolderId, String parentWorkspaceOwnerKey, String folderName, String subjectKey) {
        try {
            if (parentFolderId == null && StringUtils.isBlank(parentWorkspaceOwnerKey)) {
                return folderDao.getOrCreateDefaultTreeNodeFolder(subjectKey, folderName);
            } else {
                TreeNode parentFolder;
                if (parentFolderId != null) {
                    parentFolder = folderDao.getDomainObject(subjectKey, TreeNode.class, parentFolderId.longValue());
                    LOG.warn("No folder with id:{} owned by {}", parentFolderId, subjectKey);
                } else {
                    parentFolder = folderDao.getDefaultWorkspace(parentWorkspaceOwnerKey);
                    LOG.warn("No default workspace found for {}", parentWorkspaceOwnerKey);
                }
                if (parentFolder == null) {
                    throw new IllegalArgumentException("Parent folder not found");
                }
                TreeNode folder =  new TreeNode();
                folder.setName(folderName);
                TreeNode newFolder = folderDao.save(subjectKey, folder);
                folderDao.addChildren(subjectKey, parentFolder, ImmutableList.of(Reference.createFor(newFolder)));
                return newFolder;
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public void addImageStack(TreeNode folder, Image imageStack, String subjectKey) {
        try {
            folderDao.save(subjectKey, imageStack);
            folderDao.addChildren(subjectKey, folder, ImmutableList.of(Reference.createFor(imageStack)));
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
            folderDao.save(subjectKey, imageFile);
            folderDao.addChildren(subjectKey, folder, ImmutableList.of(Reference.createFor(imageFile)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
