package org.janelia.jacs2.asyncservice.maintenanceservices;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.dataservice.storage.DataStorageLocationFactory;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.dao.LegacyDomainDao.DaoIndex;
import org.janelia.model.domain.DomainUtils;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.tiledMicroscope.TmSample;
import org.janelia.rendering.RenderedVolumeLoader;
import org.janelia.rendering.RenderedVolumeLoaderImpl;
import org.janelia.rendering.RenderedVolumeLocation;
import org.janelia.rendering.ymlrepr.RawVolData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                        new DaoIndex("{filepath:1}"),
                        new DaoIndex("{sampleRef:1}"),
                        new DaoIndex("{libraries:1}")
                ));

        legacyDomainDao.ensureCollectionIndex("cdmipMask",
                asList(
                        new DaoIndex("{filepath:1}"),
                        new DaoIndex("{sourceSampleRef:1}"),
                        new DaoIndex("{alignmentSpace:1}")
                ));

        legacyDomainDao.ensureCollectionIndex("cdmipLibrary",
                asList(
                        new DaoIndex("{identifier:1}", "{unique:true}"),
                        new DaoIndex("{readers:1, identifier:1}")
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

    void refreshPermissions() {
        DomainUtils.getCollectionNames().stream()
                .filter(collectionName -> !collectionName.equals("tmNeuron"))
                .forEach(collectionName -> {
                    LOG.info("Refresh permissions on collection {}", collectionName);
                    legacyDomainDao.giveOwnerReadWriteToAllFromCollection(collectionName);
                });
    }

    /**
     * Refresh the filesystemSync flag for all TmSamples in the system. This should be called periodically to
     * keep the flags updated with the state of the filesystem.
     * TODO: this also performs surreptitious migration to the new schema. Can be removed later once all TmSamples
     * are migrated.
     *
     * @throws Exception
     */
    void refreshTmSampleSync() throws Exception {
        // Walk all TmSamples in the database
        for (TmSample sample : legacyDomainDao.getDomainObjects(null, TmSample.class)) {
            boolean dirty = migrateTmSample(sample);
            if (dirty) {
                // Persist the sample if anything changed
                legacyDomainDao.save(sample.getOwnerKey(), sample);
            }
        }
    }

    /**
     * Migrates the given TmSample from the deprecated filepath attribute to the 3-way filepaths.
     * Returns true if the TmSample has changed and should be persisted.
     *
     * @param sample
     * @return true if changes were made
     */
    private boolean migrateTmSample(TmSample sample) {
        String sampleFilepath;
        if (StringUtils.startsWith(sample.getFilepath(), "//")) {
            sampleFilepath = sample.getFilepath().substring(1);
        } else {
            sampleFilepath = sample.getFilepath();
        }
        boolean prevSyncFlag = sample.isFilesystemSync();
        sample.setFilesystemSync(true); // assume everything is OK until it's not
        if (sample.getFiles().isEmpty() && StringUtils.isNotBlank(sampleFilepath)) {
            // Forward migration
            LOG.info("Performing migration on {}", sample);
            LOG.info("  Setting Octree data path to {}", sampleFilepath);
            RenderedVolumeLocation rvl;
            try {
                rvl = dataStorageLocationFactory.lookupJadeDataLocation(sampleFilepath, sample.getOwnerKey(), null)
                        .map(dl -> dataStorageLocationFactory.asRenderedVolumeLocation(dl))
                        .orElseGet(() -> {
                            LOG.info("No storage location could be found for {}", sampleFilepath);
                            return null;
                        });
                if (rvl != null) {
                    sample.setLargeVolumeOctreeFilepath(sampleFilepath);
                } else {
                    sample.setFilesystemSync(false);
                }
            } catch (Exception e) {
                // no rendered volume location could be obtained so stop here
                sample.setFilesystemSync(false);
                LOG.info("  Error encountered while checking the sample volume path {}", sampleFilepath, e);
                return prevSyncFlag != sample.isFilesystemSync(); // return true only if sync flag changed
            }

            // Check KTX octree at relative location
            String ktxFullPath;
            if (StringUtils.isNotBlank(sample.getLargeVolumeKTXFilepath())) {
                ktxFullPath = sample.getLargeVolumeKTXFilepath();
            } else {
                ktxFullPath = StringUtils.appendIfMissing(sampleFilepath, "/") + "ktx";
                LOG.info("  Setting KTX data path to {}", ktxFullPath);
                DomainUtils.setFilepath(sample, FileType.LargeVolumeKTX, ktxFullPath);
            }
            try {
                boolean ktxNotFound = dataStorageLocationFactory.lookupJadeDataLocation(ktxFullPath, sample.getOwnerKey(), null)
                        .map(dl -> false)
                        .orElse(true);
                if (ktxNotFound) {
                    LOG.warn("  Could not find KTX directory for sample {} at {}", sample, ktxFullPath);
                    sample.setFilesystemSync(false);
                }
            } catch (Exception e) {
                sample.setFilesystemSync(false);
                LOG.info("  Error encountered while checking the sample KTX path {}", ktxFullPath, e);
            }

            try {
                // Check raw data
                String acquisionPath;
                if (StringUtils.isNotBlank(sample.getTwoPhotonAcquisitionFilepath())) {
                    acquisionPath = sample.getTwoPhotonAcquisitionFilepath();
                } else {
                    RenderedVolumeLoader loader = new RenderedVolumeLoaderImpl();
                    RawVolData rawVolData = loader.loadRawVolumeData(rvl);
                    if (rawVolData != null && !StringUtils.isBlank(rawVolData.getPath())) {
                        acquisionPath = rawVolData.getPath();
                        LOG.info("  Setting RAW data path to {}", rawVolData.getPath());
                        DomainUtils.setFilepath(sample, FileType.TwoPhotonAcquisition, rawVolData.getPath());
                    } else {
                        acquisionPath = null;
                    }
                }
                if (StringUtils.isNotBlank(acquisionPath)) {
                    boolean acquisitionPathNotFound = dataStorageLocationFactory.lookupJadeDataLocation(acquisionPath, sample.getOwnerKey(), null)
                            .map(dl -> false)
                            .orElse(true);
                    if (acquisitionPathNotFound) {
                        LOG.info("  Could not find the storage for tile acquisition path {}", acquisionPath);
                        sample.setFilesystemSync(false);
                    }
                }
            } catch (Exception e) {
                LOG.info("  Error encountered while looking for raw data at {}", sampleFilepath, e);
                sample.setFilesystemSync(false);
            }
            return true;
        } else if (StringUtils.isBlank(sample.getFilepath())) {
            LOG.warn("  There is no information where the sample rendered volume is located in order to perform the migration of sample {}", sample);
            return false;
        } else {
            // most likely the sample has been migrated
            LOG.info("  Sample {} has already been migrated - currently set files are {}", sample, sample.getFiles());
            return true;
        }
    }

}
