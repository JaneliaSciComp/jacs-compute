package org.janelia.jacs2.asyncservice.files;

import com.google.common.collect.ImmutableMap;
import org.janelia.jacs2.asyncservice.dataimport.ContentStack;
import org.janelia.jacs2.asyncservice.dataimport.StorageContentHelper;
import org.janelia.jacs2.asyncservice.dataimport.StorageContentObject;
import org.janelia.jacs2.asyncservice.dataimport.StorageObject;
import org.janelia.jacs2.dataservice.storage.StorageEntryInfo;
import org.janelia.model.access.domain.dao.SetFieldValueHandler;
import org.janelia.model.access.domain.dao.SyncedPathDao;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.files.SyncedPath;
import org.janelia.model.domain.files.SyncedRoot;
import org.janelia.model.domain.tiledMicroscope.TmSample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Map;

/**
 * A file discovery agent which finds Horta samples and creates corresponding TmSample objects.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("hortaDiscoveryAgent")
public class HortaDiscoveryAgent implements FileDiscoveryAgent {

    private static final Logger LOG = LoggerFactory.getLogger(StorageContentHelper.class);

    @Inject
    private SyncedPathDao syncedPathDao;

    public void discover(SyncedRoot syncedRoot, Map<String, SyncedPath> currentPaths, StorageContentObject storageObject) {

        if (storageObject.isCollection()) {
            String filepath = storageObject.getAbsolutePath().toString();
            LOG.info("Inspecting potential TM sample directory {}", filepath);

            StorageObject attributes = storageObject.resolve("transform.txt");
            if (storageObject.getHelper().exists(attributes)) {
                LOG.info("  Found transform.txt");

//                if (hasKTX(storageObject)) {
//                    LOG.info("  Found KTX imagery");
//                    sample.setLargeVolumeKTXFilepath(samplePath + "/ktx");
//                }
//                else {
//                    sample.getFiles().remove(FileType.LargeVolumeKTX);
//                }


                if (currentPaths.containsKey(filepath)) {
                    LOG.info("Updating TmSample: "+filepath);
                    TmSample n5 = (TmSample)currentPaths.get(filepath);
                    n5.setExistsInStorage(true);
                    syncedPathDao.update(n5.getId(), ImmutableMap.of(
                            "existsInStorage", new SetFieldValueHandler<>(true)));
                }
                else {
                    LOG.info("Found new TmSample: "+storageObject.getName());
                    TmSample sample = new TmSample();
                    sample.setRootRef(Reference.createFor(syncedRoot));
                    sample.setExistsInStorage(true);
                    sample.setName(storageObject.getName());
                    sample.setFilepath(filepath);



                    syncedPathDao.addSyncedPath(syncedRoot.getOwnerKey(), syncedRoot, sample);
                }
            }
        }
    }

//    /**
//     * Tests the sample with name sampleName within a storageInfo to see if it contains KTX imagery.
//     * @param storageInfo base folder
//     * @param sampleName name of the sample (subfolder)
//     * @param subjectKey subject key for access
//     * @param authToken auth key for access
//     * @return true if the sample contains KTX imagery (a 'ktx' folder with '.ktx' files)
//     */
//    private boolean hasKTX(StorageContentObject storageObject) {
//        try {
//            StorageObject ktxObject = storageObject.resolve("ktx");
//            ktxObject.getHelper().
//
//            ktxObject.getHelper().listContent(ktxObject);
//
//            String ktxPath = sampleURL+"/ktx";
//            List<ContentStack> ktx = storageContentHelper.listContent(storageInfo.getStorageURL(), ktxPath, 1, subjectKey, authToken);
//            boolean containsKtx = ktx.stream().anyMatch(k -> {
//                String ktxFilepath = k.getMainRep().getRemoteInfo().getEntryRelativePath();
//                return ktxFilepath.endsWith(".ktx");
//            });
//            if (containsKtx) {
//                return true;
//            }
//            else {
//                logger.error("Could not find KTX files for sample '{}'", sampleName);
//            }
//
//        }
//        catch (Exception e) {
//            logger.error("  Could not find KTX directory for sample '{}'", sampleName);
//        }
//
//        return false;
//    }
}
