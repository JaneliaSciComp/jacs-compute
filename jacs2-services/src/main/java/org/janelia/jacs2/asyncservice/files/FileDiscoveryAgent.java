package org.janelia.jacs2.asyncservice.files;

import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.files.SyncedPath;
import org.janelia.model.domain.files.SyncedRoot;

import java.util.Map;

/**
 * An interface for file discovery agents which produce SyncedPaths.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public interface FileDiscoveryAgent<T extends DomainObject> {

    /**
     * The agent should implement this to process the given storage object, and create or update the corresponding
     * database object, if necessary.
     *
     * @param syncedRoot the synchronized root where this storage object was found
     * @param currentPaths The currently persisted paths of the root
     * @param jadeObject the object in storage
     *
     * @return discovered object, if any
     */
    T discover(SyncedRoot syncedRoot, Map<String, SyncedPath> currentPaths, JadeObject jadeObject);

}
