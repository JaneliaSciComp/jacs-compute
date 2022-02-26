package org.janelia.jacs2.asyncservice.files;

import org.apache.commons.io.IOUtils;
import org.janelia.jacs2.asyncservice.dataimport.StorageContentHelper;
import org.janelia.jacs2.asyncservice.dataimport.StorageContentObject;
import org.janelia.jacs2.asyncservice.dataimport.StorageObject;
import org.janelia.model.access.domain.dao.SyncedPathDao;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.files.N5Container;
import org.janelia.model.domain.files.SyncedRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

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

    public void discover(SyncedRoot syncedRoot, StorageContentObject storageObject) {
        if (storageObject.isCollection() && storageObject.getName().endsWith(".n5")) {
            LOG.info("Found n5: "+storageObject);

            StorageObject attributes = storageObject.resolve("attributes.json");
            LOG.info("attributes.json exists: {}", storageObject.getHelper().exists(attributes));
            LOG.info("attributes.json size: {}", storageObject.getHelper().getContentLength(attributes));

            try {
                String result = IOUtils.toString(storageObject.getHelper().getContent(attributes), StandardCharsets.UTF_8);
                LOG.info("attributes.json content: {}", result);
            } catch (IOException e) {
                LOG.error("Error reading content", e);
            }

            N5Container n5 = new N5Container();
            n5.setRootRef(Reference.createFor(syncedRoot));
            n5.setExistsInStorage(true);
            n5.setFilepath(storageObject.getAbsolutePath().toString());

            syncedPathDao.addSyncedPath(syncedRoot.getOwnerKey(), syncedRoot, n5);
        }
    }
}
