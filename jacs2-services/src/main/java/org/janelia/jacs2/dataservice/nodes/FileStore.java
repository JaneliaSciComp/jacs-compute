package org.janelia.jacs2.dataservice.nodes;

import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.access.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A network file store with a managed directory structure.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Singleton
public class FileStore {

    private static final Logger log = LoggerFactory.getLogger(FileStore.class);
    private final String baseDir;
    private final TimebasedIdentifierGenerator idGenerator;

    @Inject
    public FileStore(@PropertyValue(name = "service.DefaultWorkingDir") String baseDir,
                     @JacsDefault TimebasedIdentifierGenerator idGenerator) {
        this.baseDir = baseDir;
        this.idGenerator = idGenerator;
    }

    /**
     * Create a new unique node in the file store, and return a handle to it.
     * @param ownerKey subject name of the node owner
     * @param nodeName top-level name of the node
     * @return
     */
    public FileStoreNode createNode(String ownerKey, String nodeName) throws IOException {
        return createNode(ownerKey, nodeName, idGenerator.generateLongId());
    }

    /**
     * Create a new node in the file store, and return a handle to it.
     * @param ownerKey subject name of the node owner
     * @param nodeName top-level name of the node
     * @param nodeId a unique identifier (GUID)
     * @return
     */
    public FileStoreNode createNode(String ownerKey, String nodeName, Long nodeId) throws IOException {
        FileStoreNode node = getNode(ownerKey, nodeName, nodeId);
        Path createdPath = Files.createDirectories(node.toPath());
        log.info("Created new node at {}", createdPath);
        return node;
    }

    /**
     * Returns a handle to a node in the file store. The node may or may not actually exist on disk.
     * @param ownerKey subject name of the node owner
     * @param nodeName top-level name of the node
     * @param nodeId a unique identifier (GUID)
     * @return
     */
    public FileStoreNode getNode(String ownerKey, String nodeName, Long nodeId) {
        return new FileStoreNode(baseDir, ownerKey, nodeName, nodeId);
    }

}
