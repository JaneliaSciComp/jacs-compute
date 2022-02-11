package org.janelia.jacs2.asyncservice.dataimport;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.dataservice.storage.StorageEntryInfo;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Builds on top of StorageContentHelper to create a better storage API by abstracting away JADE implementation details.
 */
public class BetterStorageHelper extends StorageContentHelper {

    private static final Logger LOG = LoggerFactory.getLogger(StorageContentHelper.class);

    private String subjectKey;
    private String authToken;

    /**
     *
     * @param storageService
     * @param subjectKey subject key of the user
     * @param authToken authentication token for JADE
     */
    public BetterStorageHelper(StorageService storageService, String subjectKey, String authToken) {
        super(storageService);
        this.subjectKey = subjectKey;
        this.authToken = authToken;
    }

    /**
     * Find the StorageLocation of the given path. If no storage location exists, the returned optional is empty.
     * @param path full path to locate
     * @return
     */
    public Optional<StorageLocation> lookupPath(String path) {
        return lookupStorage(path, subjectKey, authToken)
                .map(s -> new StorageLocation(Paths.get(path), Paths.get(s.getEntryRelativePath()), s));
    }

    /**
     * List the child objects of the given storage location.
     * @param location a storage location
     * @return list of children found inside the storage location
     */
    public List<StorageObject> listContent(StorageLocation location) {
        StorageEntryInfo storageInfo = location.getStorageInfo();
        LOG.debug(">>> want absolute path: {}", location.getAbsolutePath());
        LOG.debug(">>> want relative path: {}", location.getRelativePath());
        LOG.debug(">>> entry relative path: {}", storageInfo.getEntryRelativePath());
        return listContent(storageInfo.getStorageURL(), location.getRelativePath().toString(), 1, subjectKey, authToken)
                .stream()
                // Filter out the blank relative path which represents the root object,
                // since we're only interested in children
                .filter(c -> StringUtils.isNotBlank(c.getMainRep().getRemoteInfo().getEntryRelativePath()))
                // Simplify the API by wrapping
                .map(o -> new StorageObject(location, o))
                .collect(Collectors.toList());
    }

    /**
     * Add important path information to StorageEntryInfo.
     * TODO: combine with StorageObject
     */
    public class StorageLocation {

        private Path absolutePath;
        private Path relativePath;
        private StorageEntryInfo storageInfo;

        public StorageLocation(Path absolutePath, Path relativePath, StorageEntryInfo storageInfo) {
            this.absolutePath = absolutePath;
            this.relativePath = relativePath;
            this.storageInfo = storageInfo;
        }

        public Path getAbsolutePath() {
            return absolutePath;
        }

        public Path getRelativePath() {
            return relativePath;
        }

        public StorageEntryInfo getStorageInfo() {
            return storageInfo;
        }

        /**
         * Convenience function to get the containing class.
         */
        public BetterStorageHelper getHelper() {
            return BetterStorageHelper.this;
        }
    }

    /**
     * Simplify the indecipherable ContentStack API.
     */
    public class StorageObject {

        private StorageLocation location;
        private ContentStack contentStack;
        private StorageContentInfo mainRep;

        public StorageObject(StorageLocation location, ContentStack contentStack) {
            this.location = location;
            this.contentStack = contentStack;
            this.mainRep = contentStack.getMainRep();
        }

        /**
         * Does this object represent a directory which has contents that can be listed?
         * @return true if the object is a directory and can be listed
         */
        public boolean isDirectory() {
            // TODO: as far as I can tell, there's no precise way to tell if something is a file or directory using
            //  this storage API. This is a heuristic for now.
            return mainRep.getSize()==4096
                    && "application/octet-stream".equals(mainRep.getRemoteInfo().getMimeType());
        }

        /**
         * Convert this object to a storage location, so that it can be further interrogated with list operations.
         * @return a storage location representing this object
         */
        public StorageLocation toStorageLocation() {
            return new StorageLocation(getAbsolutePath(), getRelativePath(), location.getStorageInfo());
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
            return location.getAbsolutePath().resolve(getName());
        }

        /**
         * Returns the path of the object relative to the storage location.
         * @return path object
         */
        public Path getRelativePath() {
            return location.getRelativePath().resolve(getName());
        }

        /**
         * Convenience function to get the containing class.
         */
        public BetterStorageHelper getHelper() {
            return BetterStorageHelper.this;
        }
    }
}
