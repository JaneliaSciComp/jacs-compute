package org.janelia.model.access.dao.mongo;

import com.google.common.collect.Ordering;
import com.google.common.collect.Streams;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.janelia.model.access.dao.JacsServiceDataArchiveDao;
import org.janelia.model.access.dao.JacsServiceDataDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ProcessingLocation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JacsServiceDataArchiveMongoDaoITest extends AbstractMongoDaoITest<JacsServiceData> {

    private List<JacsServiceData> testData = new ArrayList<>();
    private JacsServiceDataDao testActiveServiceDao;
    private JacsServiceDataArchiveMongoDao testArchivedServiceDao;

    @Before
    public void setUp() {
        testActiveServiceDao = new JacsServiceDataMongoDao(testMongoDatabase, idGenerator);
        testArchivedServiceDao = new JacsServiceDataArchiveMongoDao(testMongoDatabase);
    }

    @After
    public void tearDown() {
        // delete the data that was created for testing
        deleteAll(testActiveServiceDao, testData);
        testData.forEach(sd -> MongoDaoHelper.delete(testArchivedServiceDao.archiveMongoCollection, sd.getId()));
    }

    @Test
    public void archiveServiceHierarchy() {
        JacsServiceData si1 = createTestService("s1", ProcessingLocation.LOCAL);
        JacsServiceData si1_1 = createTestService("s1.1", ProcessingLocation.LOCAL);
        JacsServiceData si1_2 = createTestService("s1.2", ProcessingLocation.LOCAL);
        JacsServiceData si1_3 = createTestService("s1.3", ProcessingLocation.LOCAL);
        JacsServiceData si1_2_1 = createTestService("s1.2.1", ProcessingLocation.LOCAL);
        si1.addServiceDependency(si1_1);
        si1.addServiceDependency(si1_2);
        si1_1.addServiceDependency(si1_2);
        si1_1.addServiceDependency(si1_3);
        si1_2.addServiceDependency(si1_2_1);
        testActiveServiceDao.saveServiceHierarchy(si1);
        List<JacsServiceData> s1Hierarchy = si1.serviceHierarchyStream().collect(Collectors.toList());

        s1Hierarchy.forEach(sd -> testArchivedServiceDao.archive(sd));

        s1Hierarchy.forEach(sd -> {
            JacsServiceData retrievedServiceData = testActiveServiceDao.findById(sd.getId());
            JacsServiceData archivedServiceData = testArchivedServiceDao.findArchivedEntityById(sd.getId());
            assertNull(retrievedServiceData);
            assertNotNull(archivedServiceData);
        });

        JacsServiceData archivedService = testArchivedServiceDao.findArchivedServiceHierarchy(si1.getId());
        assertNotNull(archivedService);
        List<JacsServiceData> archivedServiceHierarchy = archivedService.serviceHierarchyStream().collect(Collectors.toList());
        Comparator<JacsServiceData> sdComp = (sd1, sd2) -> {
            if (sd1.getId().equals(sd2.getId())) {
                return 0;
            } else if (sd1.getId().longValue() - sd2.getId().longValue() < 0L) {
                return  -1;
            } else {
                return 1;
            }
        };
        Streams.zip(s1Hierarchy.stream().sorted(sdComp), archivedServiceHierarchy.stream().sorted(sdComp), (sd, archivedSD) -> {
            return ImmutablePair.of(sd, archivedSD);
        }).forEach(activeArchivedPair -> {
            assertNotSame(activeArchivedPair.getLeft(), activeArchivedPair.getRight());
            assertEquals(activeArchivedPair.getLeft().getId(), activeArchivedPair.getRight().getId());
        });
    }

    @Override
    protected List<JacsServiceData> createMultipleTestItems(int nItems) {
        List<JacsServiceData> testItems = new ArrayList<>();
        for (int i = 0; i < nItems; i++) {
            testItems.add(createTestService("s" + (i + 1), i % 2 == 0 ? ProcessingLocation.LOCAL : ProcessingLocation.SGE_DRMAA));
        }
        return testItems;
    }

    private JacsServiceData createTestService(String serviceName, ProcessingLocation processingLocation) {
        JacsServiceData si = new JacsServiceData();
        si.setName(serviceName);
        si.setProcessingLocation(processingLocation);
        si.addArg("I1");
        si.addArg("I2");
        testData.add(si);
        return si;
    }

}
