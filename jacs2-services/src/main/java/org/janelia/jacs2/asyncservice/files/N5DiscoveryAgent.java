package org.janelia.jacs2.asyncservice.files;

import com.google.common.collect.ImmutableMap;
import org.janelia.jacs2.asyncservice.dataimport.StorageContentHelper;
import org.janelia.jacs2.asyncservice.dataimport.StorageContentObject;
import org.janelia.jacs2.asyncservice.dataimport.StorageObject;
import org.janelia.model.access.domain.dao.SetFieldValueHandler;
import org.janelia.model.access.domain.dao.SyncedPathDao;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.files.N5Container;
import org.janelia.model.domain.files.SyncedPath;
import org.janelia.model.domain.files.SyncedRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Map;

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

    public void discover(SyncedRoot syncedRoot, Map<String, SyncedPath> currentPaths, StorageContentObject storageObject) {
        if (storageObject.isCollection() && storageObject.getName().endsWith(".n5")) {
            StorageObject attributes = storageObject.resolve("attributes.json");
            if (storageObject.getHelper().exists(attributes)) {

                if (currentPaths.containsKey(storageObject.getName())) {
                    LOG.info("Updating N5: "+storageObject.getName());
                    N5Container n5 = (N5Container)currentPaths.get(storageObject.getName());
                    n5.setExistsInStorage(true);
                    syncedPathDao.update(n5.getId(), ImmutableMap.of(
                            "existsInStorage", new SetFieldValueHandler<>(true)));
                }
                else {
                    LOG.info("Found new N5: "+storageObject.getName());
                    N5Container n5 = new N5Container();
                    n5.setName(storageObject.getName());
                    n5.setRootRef(Reference.createFor(syncedRoot));
                    n5.setExistsInStorage(true);
                    n5.setFilepath(storageObject.getAbsolutePath().toString());
                    syncedPathDao.addSyncedPath(syncedRoot.getOwnerKey(), syncedRoot, n5);
                }
            }
        }
    }
}
