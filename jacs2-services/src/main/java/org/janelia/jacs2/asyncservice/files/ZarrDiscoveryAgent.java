package org.janelia.jacs2.asyncservice.files;

import com.google.common.collect.ImmutableMap;
import org.janelia.jacs2.asyncservice.dataimport.StorageContentHelper;
import org.janelia.jacsstorage.newclient.JadeStorageService;
import org.janelia.jacsstorage.newclient.StorageObject;
import org.janelia.model.access.domain.dao.NDContainerDao;
import org.janelia.model.access.domain.dao.SetFieldValueHandler;
import org.janelia.model.domain.files.SyncedPath;
import org.janelia.model.domain.files.SyncedRoot;
import org.janelia.model.domain.files.ZarrContainer;
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
public class ZarrDiscoveryAgent implements FileDiscoveryAgent<ZarrContainer> {

    private static final Logger LOG = LoggerFactory.getLogger(StorageContentHelper.class);

    @Inject
    private NDContainerDao ndContainerDao;

    public ZarrContainer discover(SyncedRoot syncedRoot, Map<String, SyncedPath> currentPaths, JadeObject jadeObject) {

        JadeStorageService jadeStorage = jadeObject.getJadeStorage();
        StorageObject storageObject = jadeObject.getStorageObject();

        if (storageObject.isCollection() && storageObject.getObjectName().endsWith(".zarr")) {

            // TODO: add some additional checks

            String filepath = storageObject.getAbsolutePath();
            if (currentPaths.containsKey(filepath)) {
                LOG.info("Updating Zarr: "+filepath);
                ZarrContainer zarr = (ZarrContainer)currentPaths.get(filepath);
                zarr.setExistsInStorage(true);
                ndContainerDao.update(zarr.getId(), ImmutableMap.of(
                        "existsInStorage", new SetFieldValueHandler<>(true)));
                return zarr;
            }
            else {
                LOG.info("Found new Zarr: "+storageObject.getObjectName());
                ZarrContainer zarr = new ZarrContainer();
                zarr.setAutoSynchronized(true);
                zarr.setExistsInStorage(true);
                zarr.setName(storageObject.getObjectName());
                zarr.setFilepath(storageObject.getAbsolutePath());
                ndContainerDao.saveBySubjectKey(zarr, syncedRoot.getOwnerKey());
                return zarr;
            }
        }

        return null;
    }
}
