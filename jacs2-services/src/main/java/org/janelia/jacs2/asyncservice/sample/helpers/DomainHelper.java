package org.janelia.jacs2.asyncservice.sample.helpers;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import org.janelia.jacs2.utils.CurrentService;
import org.janelia.model.access.domain.DomainDAO;
import org.janelia.model.access.domain.DomainUtils;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.sample.Image;
import org.janelia.model.domain.workspace.TreeNode;
import org.janelia.model.domain.workspace.Workspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;

/**
 * A helper class for dealing with domain objects. It know the current service state, but also keeps track of
 * an overriding "run-as" user, which can be used to influence how objects are managed.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class DomainHelper {

    private static final Logger log = LoggerFactory.getLogger(DomainHelper.class);

    public static final String TREENODE_CLASSNAME = TreeNode.class.getSimpleName();
    public static final String IMAGE_CLASSNAME = Image.class.getSimpleName();

    private String ownerKey;

    @Inject
    protected CurrentService currentService;

    @Inject
    protected DomainDAO domainDao;

    public String getOwnerKey() {
        return ownerKey!=null ? ownerKey : currentService.getOwnerKey();
    }

    public void setOwnerKey(String ownerKey) {
        this.ownerKey = ownerKey;
    }

    /**
     * Create/return a child TreeNode.
     * @param parentFolder
     * @param childName
     * @return
     * @throws Exception
     */
    public TreeNode createChildFolder(TreeNode parentFolder, String subjectKey, String childName, Integer index) throws Exception {
        if (parentFolder.getChildren()!=null) {
            for (Reference childReference : parentFolder.getChildren()) {
                if (!childReference.getTargetClassName().equals(TREENODE_CLASSNAME)) {
                    continue;
                }
                DomainObject child = domainDao.getDomainObject(subjectKey, childReference);
                if (child.getName().equals(childName)) {
                    return (TreeNode) child;
                }
            }
        }

        // We need to create a new folder
        TreeNode node = new TreeNode();
        node.setName(childName);
        node =  domainDao.save(subjectKey, node);
        List<Reference> childRef = new ArrayList<>();
        childRef.add(Reference.createFor(TreeNode.class, node.getId()));
        domainDao.addChildren(subjectKey, parentFolder, childRef, index);
        return node;
    }

    /**
     * Create a child folder or verify it exists and return it.
     * @param parentFolder
     * @param childName
     * @return
     * @throws Exception
     */
    public TreeNode createOrVerifyChildFolder(TreeNode parentFolder, String childName, boolean createIfNecessary) throws Exception {
        
        TreeNode folder = null;
        for(DomainObject domainObject : domainDao.getDomainObjects(getOwnerKey(), parentFolder.getChildren())) {
            if (domainObject instanceof TreeNode && domainObject.getName().equals(childName)) {
                TreeNode child = (TreeNode)domainObject;
                if (child.getName().equals(childName)) {
                    if (folder != null) {
                        log.warn("Unexpectedly found multiple child folders with name=" + childName+" for parent folder id="+parentFolder.getId());
                    }
                    else {
                        folder = child;
                    }
                }
            }
        }
        
        if (folder == null) {
            folder = new TreeNode();
            folder.setName(childName);
            domainDao.save(getOwnerKey(), folder);
            domainDao.addChildren(getOwnerKey(), parentFolder, Arrays.asList(Reference.createFor(folder)));
        }

        log.debug("Using childFolder with id=" + folder.getId());
        return folder;
    }
    
    /**
     * Create the given top level object set, or verify it exists and return it. 
     * @param topLevelFolderName
     * @param createIfNecessary
     * @return
     * @throws Exception
     */
    public TreeNode createOrVerifyRootEntity(String ownerKey, String topLevelFolderName, boolean createIfNecessary) throws Exception {
        TreeNode topLevelFolder = null;
        Workspace workspace = domainDao.getDefaultWorkspace(ownerKey);
        
        for(DomainObject domainObject : domainDao.getDomainObjects(ownerKey, workspace.getChildren())) {
            if (domainObject instanceof TreeNode && domainObject.getName().equals(topLevelFolderName)) {
                topLevelFolder = (TreeNode)domainObject;
                log.debug("Found existing topLevelFolder common root, name=" + topLevelFolder.getName());
                break;
            }
        }

        if (topLevelFolder == null) {
            if (createIfNecessary) {
                log.debug("Creating new topLevelFolder with name=" + topLevelFolderName);
                topLevelFolder = new TreeNode();
                topLevelFolder.setName(topLevelFolderName);
                domainDao.save(ownerKey, topLevelFolder);
                domainDao.addChildren(ownerKey, workspace, Arrays.asList(Reference.createFor(topLevelFolder)));
                log.debug("Saved top level folder as " + topLevelFolder.getId());
            } 
            else {
                throw new Exception("Could not find top-level folder by name=" + topLevelFolderName);
            }
        }

        log.debug("Using topLevelFolder with id=" + topLevelFolder.getId());
        return topLevelFolder;
    }

    /**
     * Reorders the children of a tree node in alphabetical order by name.
     * @param treeNode
     * @throws Exception
     */
    public void sortChildrenByName(TreeNode treeNode) throws Exception {
        if (treeNode==null || !treeNode.hasChildren()) return;
        final Map<Long,DomainObject> map = DomainUtils.getMapById(domainDao.getChildren(getOwnerKey(), treeNode));
        Collections.sort(treeNode.getChildren(), new Comparator<Reference>() {
            @Override
            public int compare(Reference o1, Reference o2) {
                DomainObject d1 = map.get(o1.getTargetId());
                DomainObject d2 = map.get(o2.getTargetId());
                String d1Name = d1==null?null:d1.getName();
                String d2Name = d2==null?null:d2.getName();
                return ComparisonChain.start()
                        .compare(d1Name, d2Name, Ordering.natural().nullsLast())
                        .result();
            }
        });
        domainDao.save(getOwnerKey(), treeNode);
    }
}
