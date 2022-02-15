package org.janelia.jacs2.asyncservice.files;

import org.janelia.jacs2.asyncservice.dataimport.StorageContentHelper;
import org.janelia.jacs2.asyncservice.dataimport.StorageObject;
import org.janelia.model.access.domain.dao.SyncedPathDao;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.files.N5Container;
import org.janelia.model.domain.files.SyncedRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;

/**
 * A file discovery agent which finds N5 containers and creates corresponding N5Container objects.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("n5DiscoveryAgent")
public class N5DiscoveryAgent implements FileDiscoveryAgent {

    private static final Logger LOG = LoggerFactory.getLogger(StorageContentHelper.class);

    @Inject
    private SyncedPathDao syncedPathDao;

    public void discover(String subjectKey, SyncedRoot syncedRoot, StorageObject storageObject) {
        if (storageObject.getName().endsWith(".n5")) {
            LOG.info("Found n5: "+storageObject);
            N5Container n5 = new N5Container();
            n5.setRootRef(Reference.createFor(syncedRoot));
            n5.setExistsInStorage(true);
            n5.setFilepath(storageObject.getAbsolutePath().toString());
        }
    }
}
