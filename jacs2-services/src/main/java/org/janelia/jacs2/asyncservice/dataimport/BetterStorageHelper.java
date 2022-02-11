package org.janelia.jacs2.asyncservice.dataimport;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.dataservice.storage.StorageEntryInfo;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.Collections;
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
    public Optional<StorageObject> lookupPath(String path) {
        return lookupStorage(path, subjectKey, authToken)
                // Flat map is necessary to unwrap the optional, otherwise we have Optional<Optional<StorageObject>>
                .flatMap(s -> {
                    StorageLocation location = new StorageLocation(Paths.get(path), Paths.get(s.getEntryRelativePath()), s);
                    // TODO: the storage lookup API unfortunately doesn't provide information about the requested object,
                    //   so we need to do another query here. Ideally, this should be fixed in the API.
                    StorageEntryInfo storageInfo = location.getStorageInfo();
                    LOG.info("Listing {}",storageInfo);
                    return listContent(storageInfo.getStorageURL(), location.getRelativePath().toString(), 1, subjectKey, authToken)
                            .stream()
                            // What we really want is depth=0, but that doesn't work, it returns unlimited depth instead.
                            // So we ask for depth 1 and then filter down to the only entry without a relative path.
                            .filter(c -> StringUtils.isBlank(c.getMainRep().getRemoteInfo().getEntryRelativePath()))
                            .map(cs -> {
                                LOG.info("Found {}",cs);
                                return cs;
                            })
                            .map(cs -> new StorageObject(BetterStorageHelper.this, location, null, cs)).findFirst();
                });
    }

    /**
     * List the child objects of the given storage object.
     * @param storageObject a storage object
     * @return list of children found inside the storage location
     */
    public List<StorageObject> listContent(StorageObject storageObject) {
        if (!storageObject.isCollection()) {
            // Non-directories don't have children
            return Collections.emptyList();
        }
        StorageLocation location = storageObject.getLocation();
        StorageEntryInfo storageInfo = location.getStorageInfo();
        LOG.debug(">>> want absolute path: {}", storageObject.getAbsolutePath());
        LOG.debug(">>> want relative path: {}", storageObject.getRelativePath());
        return listContent(storageInfo.getStorageURL(), storageObject.getRelativePath().toString(), 1, subjectKey, authToken)
                .stream()
                // Filter out the blank relative path which represents the root object,
                // since we're only interested in children
                .filter(c -> StringUtils.isNotBlank(c.getMainRep().getRemoteInfo().getEntryRelativePath()))
                // Simplify the API by wrapping
                .map(o -> new StorageObject(BetterStorageHelper.this, location, storageObject, o))
                .collect(Collectors.toList());
    }


}
