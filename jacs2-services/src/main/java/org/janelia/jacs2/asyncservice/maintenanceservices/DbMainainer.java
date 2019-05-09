package org.janelia.jacs2.asyncservice.maintenanceservices;

import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.domain.DomainUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class DbMainainer {
    private static final int SAMPLE_LOCK_EXPIRATION_SECONDS = 43200; // 12 hours
    private static final Logger LOG = LoggerFactory.getLogger(DbMainainer.class);

    private final LegacyDomainDao legacyDomainDao;

    @Inject
    public DbMainainer(LegacyDomainDao legacyDomainDao) {
        this.legacyDomainDao = legacyDomainDao;
    }

    void ensureIndexes() {
        List<LegacyDomainDao.DaoIndex> domainIndexes = Arrays.asList(
                new LegacyDomainDao.DaoIndex("{ownerKey:1,_id:1}"),
                new LegacyDomainDao.DaoIndex("{writers:1,_id:1}"),
                new LegacyDomainDao.DaoIndex("{readers:1,_id:1}"),
                new LegacyDomainDao.DaoIndex("{name:1}")
        );
        // Core Model (Shared)
        legacyDomainDao.ensureCollectionIndex("subject",
                Arrays.asList(
                        new LegacyDomainDao.DaoIndex("{key:1}", "{unique:true}"),
                        new LegacyDomainDao.DaoIndex("{name:1}"),
                        new LegacyDomainDao.DaoIndex("{userGroupRoles.groupKey:1}") // Support query to get all subjects in a given group
                ));

        // Fly Model
        Stream.of(
                "treeNode",
                "ontology",
                "annotation",
                "alignmentBoard",
                "compartmentSet",
                "dataSet",
                "flyLine",
                "fragment",
                "image",
                "sample",
                "containerizedService",
                "tmSample",
                "tmWorkspace",
                "tmNeuron"
                )
                .forEach(collectionName -> legacyDomainDao.ensureCollectionIndex(collectionName, domainIndexes));

        legacyDomainDao.ensureCollectionIndex("annotation",
                Arrays.asList(new LegacyDomainDao.DaoIndex("{target:1,readers:1}")));
        legacyDomainDao.ensureCollectionIndex("dataSet",
                Arrays.asList(
                        new LegacyDomainDao.DaoIndex("{identifier:1}", "{unique:true}"),
                        new LegacyDomainDao.DaoIndex("{pipelineProcesses:1}")
                ));
        legacyDomainDao.ensureCollectionIndex("fragment",
                Arrays.asList(
                        new LegacyDomainDao.DaoIndex("{separationId:1,readers:1}"),
                        new LegacyDomainDao.DaoIndex("{sampleRef:1,readers:1}")
                ));
        legacyDomainDao.ensureCollectionIndex("image",
                Arrays.asList(
                        new LegacyDomainDao.DaoIndex("{sageId:1}"),
                        new LegacyDomainDao.DaoIndex("{slideCode:1}"),
                        new LegacyDomainDao.DaoIndex("{filepath:1}"),
                        new LegacyDomainDao.DaoIndex("{sampleRef:1,readers:1}")
                ));
        legacyDomainDao.ensureCollectionIndex("sample",
                Arrays.asList(
                        new LegacyDomainDao.DaoIndex("{dataSet:1}")
                ));
        legacyDomainDao.ensureCollectionIndex("sampleLock",
                Arrays.asList(
                        new LegacyDomainDao.DaoIndex("{creationDate:1}", String.format("{expireAfterSeconds:%d}", SAMPLE_LOCK_EXPIRATION_SECONDS)),
                        new LegacyDomainDao.DaoIndex("{sampleRef:1}", "{unique:true}"),
                        new LegacyDomainDao.DaoIndex("{ownerKey:1,taskId:1,sampleRef:1}")
                ));
        legacyDomainDao.ensureCollectionIndex("containerizedService",
                Arrays.asList(
                        new LegacyDomainDao.DaoIndex("{name:1}")
                ));

        // Mouse Model
        legacyDomainDao.ensureCollectionIndex("tmWorkspace",
                Arrays.asList(
                        new LegacyDomainDao.DaoIndex("{sampleRef:1,readers:1}")
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
