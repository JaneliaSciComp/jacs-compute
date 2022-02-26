package org.janelia.jacs2.asyncservice.dataimport;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.nio.file.Path;

/**
 * Simplify the indecipherable ContentStack API.
 */
public class StorageObject {

    private BetterStorageHelper helper;
    private StorageLocation location;
    private StorageObject parent;
    private String name;

    public StorageObject(BetterStorageHelper helper, StorageLocation location, StorageObject parent, String name) {
        super();
        this.helper = helper;
        this.location = location;
        this.parent = parent;
        this.name = name;
    }

    /**
     * Convenience function to get the containing class.
     */
    public BetterStorageHelper getHelper() {
        return helper;
    }

    StorageLocation getLocation() {
        return location;
    }

    /**
     * Return the name of the object, without any path information.
     * @return name of the object
     */
    public String getName() {
        return name;
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

    public StorageObject resolve(String name) {
        return new StorageObject(helper, location, this, name);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("location", location)
                .append("parentName", parent.name)
                .append("name", name)
                .append("absolutePath", getAbsolutePath())
                .append("relativePath", getRelativePath())
                .toString();
    }

}