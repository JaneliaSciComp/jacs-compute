package org.janelia.jacs2.asyncservice.maintenanceservices;

import javax.inject.Inject;

import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.dao.LegacyDomainDao.DaoIndex;
import org.janelia.model.domain.DomainUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Arrays.asList;

public class DbMaintainer {
    private static final int SAMPLE_LOCK_EXPIRATION_SECONDS = 43200; // 12 hours
    private static final Logger LOG = LoggerFactory.getLogger(DbMaintainer.class);

    private final LegacyDomainDao legacyDomainDao;

    @Inject
    public DbMaintainer(LegacyDomainDao legacyDomainDao) {
        this.legacyDomainDao = legacyDomainDao;
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
}
