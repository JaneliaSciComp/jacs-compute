package org.janelia.jacs2.asyncservice.files;

import org.janelia.jacs2.asyncservice.dataimport.StorageObject;

/**
 * An interface for file discovery agents which produce SyncedPaths.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public interface FileDiscoveryAgent {

    /**
     * The agent should implement this to process the given storage object, and create or update the corresponding
     * database object, if necessary.
     * @param subjectKey the user who owns the database object
     * @param storageObject the object in storage
     */
    void discover(String subjectKey, StorageObject storageObject);

}
