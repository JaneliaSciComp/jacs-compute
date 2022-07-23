package org.janelia.jacs2.asyncservice.files;

import org.janelia.jacs2.asyncservice.dataimport.StorageContentHelper;
import org.janelia.jacs2.asyncservice.lvtservices.HortaDataManager;
import org.janelia.jacsstorage.newclient.JadeStorageService;
import org.janelia.jacsstorage.newclient.StorageObject;
import org.janelia.jacsstorage.newclient.StorageObjectNotFoundException;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.files.SyncedPath;
import org.janelia.model.domain.files.SyncedRoot;
import org.janelia.model.domain.tiledMicroscope.TmSample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * A file discovery agent which finds Horta samples and creates corresponding TmSample objects.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("hortaDiscoveryAgent")
public class HortaDiscoveryAgent implements FileDiscoveryAgent<TmSample> {

    private static final Logger LOG = LoggerFactory.getLogger(StorageContentHelper.class);

    @Inject
    private HortaDataManager hortaDataManager;

    public TmSample discover(SyncedRoot syncedRoot, Map<String, SyncedPath> currentPaths, JadeObject jadeObject) {

        JadeStorageService jadeStorage = jadeObject.getJadeStorage();
        StorageObject storageObject = jadeObject.getStorageObject();

        if (storageObject.isCollection()) {
            String filepath = storageObject.getAbsolutePath();
            LOG.info("Inspecting potential TM sample directory {}", filepath);

            Path transformPath = Paths.get(storageObject.getAbsolutePath(), "transform.txt");

            if (jadeStorage.exists(storageObject.getLocation(), transformPath.toString())) {
                LOG.info("  Found transform.txt");

                String sampleName = storageObject.getObjectName();

                TmSample sample = new TmSample();
                sample.setExistsInStorage(true);
                sample.setName(sampleName);
                sample.setFilepath(filepath);

                if (hasKTX(jadeObject)) {
                    LOG.info("  Found KTX imagery");
                    sample.setLargeVolumeKTXFilepath(storageObject.getAbsolutePath() + "/ktx");
                }
                else {
                    LOG.info("  Could not find KTX files");
                    sample.getFiles().remove(FileType.LargeVolumeKTX);
                }

                if (currentPaths.containsKey(filepath)) {
                    LOG.info("Updating TmSample: "+filepath);
                    TmSample existingSample = (TmSample)currentPaths.get(filepath);
                    try {
                        // Copy discovered properties to existing sample
                        existingSample.setExistsInStorage(true);
                        for(FileType fileType : sample.getFiles().keySet()) {
                            existingSample.getFiles().put(fileType, sample.getFiles().get(fileType));
                        }
                        // Update database
                        hortaDataManager.updateSample(syncedRoot.getOwnerKey(), existingSample);
                        LOG.info("  Updated TM sample: {}", existingSample);
                        return existingSample;
                    }
                    catch (Exception e) {
                        LOG.error("  Could not update TM sample "+existingSample, e);
                    }
                }
                else {
                    LOG.info("Found new TmSample: " + sampleName);
                    try {
                        TmSample savedSample = hortaDataManager.createTmSample(syncedRoot.getOwnerKey(), sample);
                        LOG.info("  Created TM sample: {}", savedSample);
                        return savedSample;
                    }
                    catch (Exception e) {
                        LOG.error("  Could not create TM sample for " + sampleName, e);
                    }
                }
            }
        }

        return null;
    }

    private boolean hasKTX(JadeObject jadeObject) {

        JadeStorageService jadeStorage = jadeObject.getJadeStorage();
        StorageObject storageObject = jadeObject.getStorageObject();
        Path ktxPath = Paths.get(storageObject.getAbsolutePath(), "ktx");

        try {
            return jadeStorage.getChildren(storageObject.getLocation(), ktxPath.toString())
                    .stream().anyMatch(k -> k.getAbsolutePath().endsWith(".ktx"));
        }
        catch (StorageObjectNotFoundException e) {
            return false;
        }
    }
}
