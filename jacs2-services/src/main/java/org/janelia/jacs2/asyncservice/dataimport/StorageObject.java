package org.janelia.jacs2.asyncservice.dataimport;

import java.nio.file.Path;

/**
 * Simplify the indecipherable ContentStack API.
 */
public class StorageObject {

    private BetterStorageHelper helper;
    private StorageLocation location;
    private StorageObject parent;
    private StorageContentInfo mainRep;

    public StorageObject(BetterStorageHelper helper, StorageLocation location, StorageObject parent, ContentStack contentStack) {
        this.helper = helper;
        this.location = location;
        this.parent = parent;
        this.mainRep = contentStack.getMainRep();
    }

    /**
     * Does this object represent a directory which has contents that can be listed?
     * @return true if the object is a directory and can be listed
     */
    public boolean isCollection() {
        return mainRep.getRemoteInfo().isCollection();
    }

    /**
     * Return the name of the object, without any path information.
     * @return name of the object
     */
    public String getName() {
        return mainRep.getRemoteInfo().getEntryRelativePath();
    }

    /**
     * Returns the full path of object.
     * @return path object
     */
    public Path getAbsolutePath() {
        return parent == null ? location.getAbsolutePath().resolve(getName()) : parent.getAbsolutePath().resolve(getName());
    }

    /**
     * Returns the path of the object relative to the storage location.
     * @return path object
     */
    public Path getRelativePath() {
        return parent == null ? location.getRelativePath().resolve(getName()) : parent.getRelativePath().resolve(getName());
    }

    StorageLocation getLocation() {
        return location;
    }

    /**
     * Convenience function to get the containing class.
     */
    public BetterStorageHelper getHelper() {
        return helper;
    }

    @Override
    public String toString() {
        return getAbsolutePath().toString();
    }
}