package org.janelia.jacs2.asyncservice.maintenanceservices;

import java.io.File;
import java.nio.file.Paths;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.dataservice.rendering.RenderedVolumeLocationFactory;
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
    private final RenderedVolumeLocationFactory renderedVolumeLocationFactory;

    @Inject
    public DbMaintainer(LegacyDomainDao legacyDomainDao, RenderedVolumeLocationFactory renderedVolumeLocationFactory) {
        this.legacyDomainDao = legacyDomainDao;
        this.renderedVolumeLocationFactory = renderedVolumeLocationFactory;
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
                        new DaoIndex("{filepath:1}", "{unique:true}")
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
                    legacyDomainDao.giveOwnerReadWritToAllFromCollection(collectionName);
                });
    }

    /**
     * Refresh the filesystemSync flag for all TmSamples in the system. This should be called periodically to
     * keep the flags updated with the state of the filesystem.
     * TODO: this also performs surreptitious migration to the new schema. Can be removed later once all TmSamples
     * are migrated.
     * @throws Exception
     */
    void refreshTmSampleSync() throws Exception {
        // Walk all TmSamples in the database
        for (TmSample sample : legacyDomainDao.getDomainObjects(null, TmSample.class)) {
            boolean dirty = migrateTmSample(sample);
            dirty |= refreshTmSampleSync(sample);
            if (dirty) {
                // Persist the sample if anything changed
                legacyDomainDao.save(sample.getOwnerKey(), sample);
            }
        }
    }

    /**
     * Migrates the given TmSample from the deprecated filepath attribute to the 3-way filepaths.
     * Returns true if the TmSample has changed and should be persisted.
     * @param sample
     * @return true if changes were made
     */
    private boolean migrateTmSample(TmSample sample) {

        if (sample.getFiles().isEmpty() && sample.getFilepath()!=null) {
            // Forward migration
            LOG.info("Performing migration on {}", sample);

            String filepath = sample.getFilepath();
            sample.setLargeVolumeOctreeFilepath(filepath);
            LOG.info("  Setting Octree data path to {}", filepath);

            if (exists(filepath)) {

                // Find raw data
                try {
                    RenderedVolumeLocation rvl = renderedVolumeLocationFactory.getVolumeLocation(filepath, sample.getOwnerKey(), null);
                    RenderedVolumeLoader loader = new RenderedVolumeLoaderImpl();
                    RawVolData rawVolData = loader.loadRawVolumeData(rvl);
                    if (rawVolData != null && !StringUtils.isBlank(rawVolData.getPath())) {
                        if (exists(rawVolData.getPath())) {
                            LOG.info("  Setting RAW data path to {}", rawVolData.getPath());
                            DomainUtils.setFilepath(sample, FileType.TwoPhotonAcquisition, rawVolData.getPath());
                        } else {
                            LOG.info("  Directory listed in tilebase.cache.yml does not exist: {}", rawVolData.getPath());
                        }
                    } else {
                        LOG.info("  Could not find RAW directory in tilebase.cache.yml");
                    }
                }
                catch (Exception e) {
                    LOG.info("  Error encountered while looking for raw data at "+filepath, e);
                }
            }
            else {
                LOG.warn("  Could not find Octree directory at {}", filepath);
            }

            // Find KTX octree at relative location
            String ktxDir = Paths.get(filepath, "ktx").toString();
            if (exists(ktxDir)) {
                LOG.info("  Setting KTX data path to {}", ktxDir);
                DomainUtils.setFilepath(sample, FileType.LargeVolumeKTX, ktxDir);
            }
            else {
                LOG.warn("  Could not find KTX directory at {}", ktxDir);
            }

            return true;
        }

        return false;
    }

    /**
     * Refresh the filesystemSync parameter for the given TmSample.
     * Returns true if the TmSample has changed and should be persisted.
     * @param sample
     * @return true if changes were made
     */
    public boolean refreshTmSampleSync(TmSample sample) {

        LOG.info("Checking {}", sample);

        boolean sync = true;
        for (String filepath : sample.getFiles().values()) {
            boolean filepathExists = exists(filepath);
            LOG.info("  {} {}", filepath, filepathExists?"exists":"does not exist");
            if (!filepathExists) {
                sync = false;
            }
        }

        if (sample.isFilesystemSync()!=sync) {
            LOG.info("  Updating {} with sync={}", sample, sync);
            sample.setFilesystemSync(sync);
            return true;
        }

        return false;
    }

    private boolean exists(String filepath) {
        // TODO: this existence check should use JADE
        File file = new File(filepath);
        return file.exists();
    }
}
