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
 * A file discovery agent which finds Zarr containers and creates corresponding ZarrContainer objects.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("zarrDiscoveryAgent")
public class ZarrDiscoveryAgent implements FileDiscoveryAgent {

    private static final Logger LOG = LoggerFactory.getLogger(StorageContentHelper.class);

    @Inject
    private SyncedPathDao syncedPathDao;

    public void discover(SyncedRoot syncedRoot, Map<String, SyncedPath> currentPaths, StorageContentObject storageObject) {
        if (storageObject.isCollection() && storageObject.getName().endsWith(".zarr")) {
            StorageObject attributes = storageObject.resolve("attributes.json");
            if (storageObject.getHelper().exists(attributes)) {

                String filepath = storageObject.getAbsolutePath().toString();
                if (currentPaths.containsKey(filepath)) {
                    LOG.info("Updating N5: "+filepath);
                    N5Container n5 = (N5Container)currentPaths.get(filepath);
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
