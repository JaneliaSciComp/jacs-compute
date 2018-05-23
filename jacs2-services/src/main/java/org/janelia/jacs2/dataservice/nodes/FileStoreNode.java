package org.janelia.jacs2.dataservice.nodes;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * A file node lives in a file store and belongs to a certain user. It has a path like this:
 * /nrs/jacs/jacsData/filestore/nerna/Separation/166/053/2352081853550166053
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class FileStoreNode {

    protected String storePath;
    protected String username;
    protected String type;
    protected Long id;

    public FileStoreNode(String storePath, String username, String type, Long id) {
        this.storePath = storePath;
        this.username = username;
        this.type = type;
        this.id = id;
    }

    public String getStorePath() {
        return storePath;
    }

    public String getUsername() {
        return username;
    }

    public String getType() {
        return type;
    }

    public Long getId() {
        return id;
    }

    public FileStoreNode withChangedOwner(String newOwner) {
        return new FileStoreNode(storePath, newOwner, type, id);
    }

    public Path toPath() {
        String idAsString = id.toString();
        int length = idAsString.length();
        String loc1 = idAsString.substring(length - 6, length - 3);
        String loc2 = idAsString.substring(length - 3);
        return Paths.get(storePath, username, type, loc1, loc2, idAsString);
    }

    public FileStorePath toFileStorePath(String... pathElements) {
        String rest = String.join(File.separator, pathElements);
        return new FileStorePath(toPath().toString(), this, rest);
    }

    @Override
    public String toString() {
        return "FileStoreNode[" + toPath() + ']';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FileStoreNode that = (FileStoreNode) o;

        if (!storePath.equals(that.storePath)) return false;
        if (!username.equals(that.username)) return false;
        if (!type.equals(that.type)) return false;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        int result = storePath.hashCode();
        result = 31 * result + username.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + id.hashCode();
        return result;
    }
}