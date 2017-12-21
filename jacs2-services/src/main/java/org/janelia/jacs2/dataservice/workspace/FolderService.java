package org.janelia.jacs2.dataservice.workspace;

import com.google.common.collect.ImmutableList;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.domain.sample.Image;
import org.janelia.model.domain.workspace.TreeNode;
import org.janelia.model.domain.Reference;

import javax.inject.Inject;

public class FolderService {

    private final LegacyDomainDao folderDao;

    @Inject
    public FolderService(LegacyDomainDao folderDao) {
        this.folderDao = folderDao;
    }

    public TreeNode createFolder(Number parentFolderId, String folderName, String subjectKey) {
        try {
            if (parentFolderId == null) {
                return folderDao.getOrCreateDefaultFolder(subjectKey, folderName);
            } else {
                TreeNode parentFolder = folderDao.getDomainObject(subjectKey, TreeNode.class, parentFolderId.longValue());
                if (parentFolder == null) {
                    throw new IllegalArgumentException("No folder found for " + parentFolderId + " owned by " + subjectKey);
                }
                TreeNode folder =  new TreeNode();
                folder.setName(folderName);
                TreeNode newFolder = folderDao.save(subjectKey, folder);
                folderDao.addChildren(subjectKey, parentFolder, ImmutableList.of(Reference.createFor(parentFolder)));
                return newFolder;
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public void addImageFile(TreeNode folder, String imageFileName, String subjectKey) {
        try {
            Image imageFile = new Image();
            imageFile.setName(FileUtils.getFileName(imageFileName));
            imageFile.setFilepath(imageFileName);
            folderDao.save(subjectKey, imageFile);
            folderDao.addChildren(subjectKey, folder, ImmutableList.of(Reference.createFor(imageFile)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
