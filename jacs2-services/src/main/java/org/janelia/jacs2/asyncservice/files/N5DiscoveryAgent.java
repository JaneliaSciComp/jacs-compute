package org.janelia.jacs2.asyncservice.files;

import com.google.common.collect.ImmutableMap;
import org.janelia.jacsstorage.clients.api.JadeStorageService;
import org.janelia.jacsstorage.clients.api.StorageObject;
import org.janelia.model.access.domain.dao.NDContainerDao;
import org.janelia.model.access.domain.dao.SetFieldValueHandler;
import org.janelia.model.domain.files.N5Container;
import org.janelia.model.domain.files.SyncedPath;
import org.janelia.model.domain.files.SyncedRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * A file discovery agent which finds N5 containers and creates corresponding N5Container objects.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("n5DiscoveryAgent")
public class N5DiscoveryAgent implements FileDiscoveryAgent<N5Container> {

    private static final Logger LOG = LoggerFactory.getLogger(N5DiscoveryAgent.class);

    @Inject
    private NDContainerDao ndContainerDao;

    public N5Container discover(SyncedRoot syncedRoot, Map<String, SyncedPath> currentPaths, JadeObject jadeObject) {

        JadeStorageService jadeStorage = jadeObject.getJadeStorage();
        StorageObject storageObject = jadeObject.getStorageObject();

        if (storageObject.isCollection() && storageObject.getObjectName().endsWith(".n5")) {

            Path attributesPath = Paths.get(storageObject.getAbsolutePath(), "attributes.json");
            if (jadeStorage.exists(storageObject.getLocation(), attributesPath.toString())) {

                String filepath = storageObject.getAbsolutePath();
                if (currentPaths.containsKey(filepath)) {
                    LOG.info("Updating N5: "+filepath);
                    N5Container n5 = (N5Container)currentPaths.get(filepath);
                    n5.setExistsInStorage(true);
                    ndContainerDao.update(n5.getId(), ImmutableMap.of(
                            "existsInStorage", new SetFieldValueHandler<>(true)));
                    return n5;
                }
                else {
                    LOG.info("Found new N5: "+storageObject.getObjectName());
                    N5Container n5 = new N5Container();
                    n5.setAutoSynchronized(true);
                    n5.setExistsInStorage(true);
                    n5.setName(storageObject.getObjectName());
                    n5.setFilepath(storageObject.getAbsolutePath());
                    ndContainerDao.saveBySubjectKey(n5, syncedRoot.getOwnerKey());
                    return n5;
                }
            }
        }

        return null;
    }
}
