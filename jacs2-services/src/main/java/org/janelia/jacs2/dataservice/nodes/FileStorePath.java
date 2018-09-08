package org.janelia.jacs2.dataservice.nodes;

import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A path within a FileStoreNode, for example:
 * /nrs/jacs/jacsData/filestore/nerna/Separation/166/053/2352081853550166053/separate
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class FileStorePath {

    protected String filepath;
    protected FileStoreNode node;
    protected String rest;

    public FileStorePath(String filepath, FileStoreNode node, String rest) {
        this.filepath = filepath;
        this.node = node;
        this.rest = rest;
    }

    public String getFilepath() {
        return filepath;
    }

    public FileStoreNode getFileNode() {
        return node;
    }

    public String getStorePath() {
        return node.getStorePath();
    }

    public String getUsername() {
        return node.getUsername();
    }

    public String getType() {
        return node.getType();
    }

    public Long getId() {
        return node.getId();
    }

    public String getRest() {
        return rest;
    }

    public Path toPath() {
        return node.toPath().resolve(rest);
    }

    public String getFilepath(boolean stripSubPath) {
        Path path = node.toPath();
        if (stripSubPath) {
            return path.toString();
        }
        else {
            return path.resolve(rest).toString();
        }
    }

    /**
     * Get the corresponding node in a different owners' file store.
     * @param newOwner the new owner's username
     * @return
     */
    public FileStorePath withChangedOwner(String newOwner) {
        FileStoreNode changedNode = node.withChangedOwner(newOwner);
        return changedNode.toFileStorePath(rest);
    }

    @Override
    public String toString() {
        return "FileStorePath[" +
                "node=" + node +
                ", rest='" + rest + '\'' +
                ']';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FileStorePath that = (FileStorePath) o;

        if (!node.equals(that.node)) return false;
        return rest != null ? rest.equals(that.rest) : that.rest == null;
    }

    @Override
    public int hashCode() {
        int result = node.hashCode();
        result = 31 * result + (rest != null ? rest.hashCode() : 0);
        return result;
    }

    private static final Pattern pattern = Pattern.compile("(.*?)/(\\w+)/(\\w+)/(\\d{3})/(\\d{3})/(\\d+)/?(.*?)");

    /**
     * Parses the given filepath and returns a FileStorePath instance.
     * @param filepath
     * @return
     */
    public static FileStorePath parseFilepath(String filepath) {

        try {
            Matcher m = pattern.matcher(filepath);
            if (!m.matches()) {
                throw new IllegalArgumentException("Cannot parse path as a filestore path: "+filepath);
            }

            String storePath = m.group(1);
            String username = m.group(2);
            String type = m.group(3);
            Long id = new Long(m.group(6));
            FileStoreNode node = new FileStoreNode(storePath, username, type, id);
            String rest = m.group(7);

            return new FileStorePath(filepath, node, rest);
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse path as a filestore path: "+filepath, e);
        }
    }
}
