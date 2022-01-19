package org.janelia.jacs2.asyncservice.maintenanceservices;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.dataservice.storage.DataStorageLocationFactory;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.dao.LegacyDomainDao.DaoIndex;
import org.janelia.model.domain.DomainUtils;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.tiledMicroscope.TmSample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;

public class DbMaintainer {
    private static final int SAMPLE_LOCK_EXPIRATION_SECONDS = 43200; // 12 hours
    private static final Logger LOG = LoggerFactory.getLogger(DbMaintainer.class);

    private final LegacyDomainDao legacyDomainDao;
    private final DataStorageLocationFactory dataStorageLocationFactory;

    @Inject
    public DbMaintainer(LegacyDomainDao legacyDomainDao, DataStorageLocationFactory dataStorageLocationFactory) {
        this.legacyDomainDao = legacyDomainDao;
        this.dataStorageLocationFactory = dataStorageLocationFactory;
    }

    void ensureIndexes() {

        // Subject model

        legacyDomainDao.ensureCollectionIndex("subject",
                asList(
                        new DaoIndex("{key:1}", "{unique:true}"),
                        new DaoIndex("{name:1}"),
                        new DaoIndex("{userGroupRoles.groupKey:1}") // Support query to get all subjects in a given group
                ));

        // Standard indexing for all domain objects

        DomainUtils.getCollectionNames().forEach(collectionName -> legacyDomainDao.ensureCollectionIndex(collectionName,
                asList(
                        // Compound indexes allow for query on any prefix, so we don't need separate indexes on readers and writers
                        new DaoIndex("{ownerKey:1,_id:1}"),
                        new DaoIndex("{writers:1,_id:1}"),
                        new DaoIndex("{readers:1,_id:1}"),
                        new DaoIndex("{name:1}"))
        ));

        legacyDomainDao.ensureCollectionIndex("annotation",
                asList(
                        new DaoIndex("{target:1,readers:1}")
                ));

        legacyDomainDao.ensureCollectionIndex("dataSet",
                asList(
                        new DaoIndex("{identifier:1}", "{unique:true}"),
                        new DaoIndex("{readers:1, identifier:1}"),
                        new DaoIndex("{pipelineProcesses:1}")
                ));

        legacyDomainDao.ensureCollectionIndex("fragment",
                asList(
                        new DaoIndex("{separationId:1,readers:1}"),
                        new DaoIndex("{sampleRef:1,readers:1}")
                ));

        legacyDomainDao.ensureCollectionIndex("image",
                asList(
                        new DaoIndex("{sageId:1}"),
                        new DaoIndex("{slideCode:1}"),
                        new DaoIndex("{filepath:1}"),
                        new DaoIndex("{sampleRef:1,readers:1}")
                ));

        legacyDomainDao.ensureCollectionIndex("sample",
                asList(
                        new DaoIndex("{dataSet:1}")
                ));

        legacyDomainDao.ensureCollectionIndex("sampleLock",
                asList(
                        new DaoIndex("{creationDate:1}", String.format("{expireAfterSeconds:%d}", SAMPLE_LOCK_EXPIRATION_SECONDS)),
                        new DaoIndex("{sampleRef:1}", "{unique:true}"),
                        new DaoIndex("{ownerKey:1,taskId:1,sampleRef:1}")
                ));

        legacyDomainDao.ensureCollectionIndex("containerizedService",
                asList(
                        new DaoIndex("{name:1,version:1}")
                ));

        legacyDomainDao.ensureCollectionIndex("cdmipImage",
                asList(
                        new DaoIndex("{filepath:1}", "{unique:true}"),
                        new DaoIndex("{sampleRef:1}"),
                        new DaoIndex("{libraries:1}"),
                        new DaoIndex("{libraries:1,alignmentSpace:1}")
                ));

        legacyDomainDao.ensureCollectionIndex("cdmipMask",
                asList(
                        new DaoIndex("{filepath:1}", "{unique:true}"),
                        new DaoIndex("{sourceSampleRef:1}"),
                        new DaoIndex("{alignmentSpace:1}")
                ));

        legacyDomainDao.ensureCollectionIndex("cdmipLibrary",
                asList(
                        new DaoIndex("{identifier:1}", "{unique:true}"),
                        new DaoIndex("{readers:1, identifier:1}")
                ));

//        legacyDomainDao.ensureCollectionIndex("emDataSet",
//                asList(
//                        new DaoIndex("{identifier:1}", "{unique:true}"),
//                        new DaoIndex("{readers:1, identifier:1}")
//                ));

        legacyDomainDao.ensureCollectionIndex("emBody",
                asList(
                        new DaoIndex("{dataSetIdentifier:1}"),
                        new DaoIndex("{readers:1, dataSetIdentifier:1}"),
                        new DaoIndex("{neuronType:1}")
                ));

        // Mouse Model

        legacyDomainDao.ensureCollectionIndex("tmWorkspace",
                asList(
                        new DaoIndex("{sampleRef:1}"),
                        new DaoIndex("{sampleRef:1,readers:1}")
                ));

        legacyDomainDao.ensureCollectionIndex("tmNeuron",
                asList(
                        new DaoIndex("{workspaceRef:1}"),
                        new DaoIndex("{workspaceRef:1,readers:1}")
                ));
    }

    void refreshPermissions(boolean includeFragments, boolean includeNeurons) {
        DomainUtils.getCollectionNames().stream()
                .filter(collectionName -> !collectionName.equals("tmNeuron") || includeNeurons)
                .filter(collectionName -> !collectionName.equals("fragment") || includeFragments)
                .forEach(collectionName -> {
                    LOG.info("Refresh permissions on collection {}", collectionName);
                    legacyDomainDao.giveOwnerReadWriteToAllFromCollection(collectionName);
                });
    }

    /**
     * Refresh the filesystemSync flag for all TmSamples in the system. This should be called periodically to
     * keep the flags updated with the state of the filesystem.
     *
     * @throws Exception
     */
    void refreshTmSampleSync() throws Exception {
        // Walk all TmSamples in the database
        for (TmSample sample : legacyDomainDao.getDomainObjects(null, TmSample.class)) {
            boolean dirty = refreshTmSampleSync(sample);
            if (dirty) {
                // Persist the sample if anything changed
                legacyDomainDao.save(sample.getOwnerKey(), sample);
            }
        }
    }

    /**
     * Refresh the filesystemSync parameter for the given TmSample.
     * Returns true if the TmSample has changed and should be persisted.
     * @param sample
     * @return true if changes were made
     */
    public boolean refreshTmSampleSync(TmSample sample) {

        LOG.info("Checking {} (filesystemSync={})", sample, sample.isFilesystemSync());

        boolean sync = sample.isFilesystemSync();
        Set<FileType> emptyPaths = new HashSet<>();
        for (FileType fileType : sample.getFiles().keySet()) {
            String filepath = sample.getFiles().get(fileType);
            if (StringUtils.isBlank(filepath)) {
                emptyPaths.add(fileType);
            }
            else {
                try {
                    // TODO: this is not correct and needs to be fixed
                    boolean filepathExists = dataStorageLocationFactory.lookupJadeDataLocation(
                            filepath, sample.getOwnerKey(), null).isPresent();
                    LOG.info("  {} {}", filepath, filepathExists ? "exists" : "does not exist");
                    if (!filepathExists) {
                        sync = false;
                    }
                } catch (Exception e) {
                    LOG.info("  Error encountered while checking the sample volume path {}", filepath, e);
                }
            }
        }

        boolean dirty = false;

        if (!emptyPaths.isEmpty()) {
            // Remove all the paths that were empty
            LOG.info("  Updating {} to remove empty file paths: {}", sample, emptyPaths);
            emptyPaths.forEach(t -> sample.getFiles().remove(t));
            dirty = true;
        }

        if (sample.isFilesystemSync()!=sync) {
            LOG.info("  Updating {} with filesystemSync={}", sample, sync);
            sample.setFilesystemSync(sync);
            dirty = true;
        }

        return dirty;
    }
}
