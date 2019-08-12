package org.janelia.model.access.dao.mongo;

import com.google.common.collect.ImmutableList;
import org.hamcrest.beans.HasPropertyWithValue;
import org.janelia.model.domain.Reference;
import org.janelia.model.jacs2.dao.mongo.SampleMongoDao;
import org.janelia.model.jacs2.domain.sample.PipelineResult;
import org.janelia.model.jacs2.domain.sample.SamplePipelineRun;
import org.janelia.model.jacs2.dao.SampleDao;
import org.janelia.model.jacs2.domain.enums.FileType;
import org.janelia.model.jacs2.domain.sample.Sample;
import org.janelia.model.jacs2.domain.sample.ObjectiveSample;
import org.janelia.model.jacs2.domain.sample.SampleTile;
import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.jacs2.SetFieldValueHandler;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.jacs2.page.SortCriteria;
import org.janelia.model.jacs2.page.SortDirection;
import org.janelia.model.jacs2.DomainModelUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.same;

public class SampleMongoDaoITest extends AbstractDomainObjectDaoITest<Sample> {

    private volatile int nextResultId = 1;
    private List<Sample> testData = new ArrayList<>();
    private SampleDao testDao;

    @Before
    public void setUp() {
        testDao = new SampleMongoDao(testMongoDatabase, idGenerator);
    }

    @After
    public void tearDown() {
        // delete the data that was created for testing
        deleteAll(testDao, testData);
    }

    @Test
    public void findAll() {
        List<Sample> testSamples = ImmutableList.of(
            createTestSample("ds1", "sc1"),
            createTestSample("ds1", "sc2"),
            createTestSample("ds1", "sc3"),
            createTestSample("ds1", "sc4"),
            createTestSample("ds1", "sc5"),
            createTestSample("ds2", "sc1"),
            createTestSample("ds2", "sc2"),
            createTestSample("ds2", "sc3"),
            createTestSample("ds2", "sc4"),
            createTestSample("ds2", "sc5")
        );
        testSamples.parallelStream().forEach(s -> testDao.save(s));
        PageRequest pageRequest = new PageRequest();
        pageRequest.setPageNumber(0);
        pageRequest.setPageSize(5);
        pageRequest.setSortCriteria(ImmutableList.of(
                new SortCriteria("dataSet", SortDirection.ASC),
                new SortCriteria("slideCode", SortDirection.DESC),
                new SortCriteria("completionDate", SortDirection.DESC)));
        PageResult<Sample> res1 = testDao.findAll(pageRequest);
        assertThat(res1.getResultList(), hasSize(5));
        assertThat(res1.getResultList(), everyItem(hasProperty("dataSet", equalTo("ds1"))));
        assertThat(res1.getResultList().stream().map(s -> s.getId()).collect(Collectors.toList()), contains(
                testSamples.get(4).getId(),
                testSamples.get(3).getId(),
                testSamples.get(2).getId(),
                testSamples.get(1).getId(),
                testSamples.get(0).getId()
        ));
        pageRequest.setPageNumber(1);
        PageResult<Sample> res2 = testDao.findAll(pageRequest);
        assertThat(res2.getResultList(), hasSize(5));
        assertThat(res2.getResultList(), everyItem(hasProperty("dataSet", equalTo("ds2"))));
        assertThat(res2.getResultList().stream().map(s -> s.getId()).collect(Collectors.toList()), contains(
                testSamples.get(9).getId(),
                testSamples.get(8).getId(),
                testSamples.get(7).getId(),
                testSamples.get(6).getId(),
                testSamples.get(5).getId()
        ));
    }

    @Test
    public void findByOwnerWithoutSubject() {
        findByOwner(null, testDao);
    }

    @Test
    public void findByIdsWithNoSubject() {
        findByIdsWithNoSubject(testDao);
    }

    @Test
    public void findByIdsWithSubject() {
        findByIdsWithSubject(testDao);
    }

    @Test
    public void findMatchingSamples() {
        Calendar currentCal = Calendar.getInstance();
        currentCal.add(Calendar.HOUR, -24);
        Date startDate = currentCal.getTime();
        List<Sample> testSamples = ImmutableList.of(
                createTestSample("ds1", "sc1"),
                createTestSample("ds1", "sc2"),
                createTestSample("ds1", "sc3"),
                createTestSample("ds1", "sc4"),
                createTestSample("ds1", "sc5"),
                createTestSample("ds2", "sc1"),
                createTestSample("ds2", "sc2"),
                createTestSample("ds2", "sc3"),
                createTestSample("ds2", "sc4"),
                createTestSample("ds2", "sc5")
        );
        currentCal.add(Calendar.HOUR, 48);
        Date endDate = currentCal.getTime();
        testDao.saveAll(testSamples);
        Sample testRequest = new Sample();
        testRequest.setDataSet("ds1");

        PageRequest pageRequest = new PageRequest();
        pageRequest.setPageNumber(1);
        pageRequest.setPageSize(3);
        PageResult<Sample> retrievedSamples;

        retrievedSamples = testDao.findMatchingSamples(null, testRequest, new DataInterval<>(startDate, endDate), pageRequest);
        assertThat(retrievedSamples.getResultList(), hasSize(2)); // only 2 items are left on the last page
        assertThat(retrievedSamples.getResultList(), everyItem(hasProperty("dataSet", equalTo("ds1"))));
    }

    @Test
    public void persistSample() {
        Sample testSample = createTestSample("ds1", "sc1");
        testSample.getObjectiveSamples().addAll(ImmutableList.of(
                createSampleObjective("o1"),
                createSampleObjective("o2"),
                createSampleObjective("o3")));
        testDao.save(testSample);
        Sample retrievedSample = testDao.findById(testSample.getId());
        assertThat(retrievedSample, not(nullValue(Sample.class)));
        assertThat(retrievedSample, not(same(testSample)));
        assertThat(retrievedSample.getId(), equalTo(testSample.getId()));
    }

    @Test
    public void updateSample() {
        Sample testSample = createTestSample("ds1", "sc1");
        testDao.save(testSample);
        Sample newSample = testDao.findById(testSample.getId());
        changeAndUpdateSample(newSample);
        Sample retrievedSample = testDao.findById(testSample.getId());
        assertNull(retrievedSample.getFlycoreAlias());
        assertThat(retrievedSample, hasProperty("line", equalTo("Updated line")));
        assertThat(retrievedSample, hasProperty("dataSet", equalTo(newSample.getDataSet())));
    }

    @Test
    public void tryToUpdateALockedSampleWithoutTheKey() {
        Sample testSample = createTestSample("ds1", "sc1");
        testSample.setLockKey("LockKey");
        testDao.save(testSample);
        Sample newSample = testDao.findById(testSample.getId());
        for (String lockKey : new String[]{null, "WrongKey"}) {
            newSample.setLockKey(lockKey);
            changeAndUpdateSample(newSample);
            Sample retrievedSample = testDao.findById(testSample.getId());
            assertThat(retrievedSample.getFlycoreAlias(), allOf(equalTo(testSample.getFlycoreAlias()), not(equalTo(newSample.getFlycoreAlias()))));
            assertThat(retrievedSample.getLine(), allOf(equalTo(testSample.getLine()), not(equalTo(newSample.getLine()))));
            assertThat(retrievedSample.getDataSet(), allOf(equalTo(testSample.getDataSet()), not(equalTo(newSample.getDataSet()))));
        }
    }

    @Test
    public void tryToUpdateALockedSampleWithTheRightKey() {
        Sample testSample = createTestSample("ds1", "sc1");
        testSample.setLockKey("LockKey");
        testDao.save(testSample);
        Sample newSample = testDao.findById(testSample.getId());
        changeAndUpdateSample(newSample);
        Sample retrievedSample = testDao.findById(testSample.getId());
        assertThat(retrievedSample.getFlycoreAlias(), allOf(equalTo(newSample.getFlycoreAlias()), not(equalTo(testSample.getFlycoreAlias()))));
        assertThat(retrievedSample.getLine(), allOf(equalTo(newSample.getLine()), not(equalTo(testSample.getLine()))));
        assertThat(retrievedSample.getDataSet(), allOf(equalTo(newSample.getDataSet()), not(equalTo(testSample.getDataSet()))));
    }

    private void changeAndUpdateSample(Sample testSample) {
        testSample.setFlycoreAlias(null);
        testSample.setDataSet("newDataSet that has been changed");
        testSample.setLine("Updated line");
        testSample.setEffector("best effector");
        testSample.getObjectiveSamples().addAll(ImmutableList.of(
                createSampleObjective("new_o1"),
                createSampleObjective("new_o2"),
                createSampleObjective("new_o3")));
        testSample.setUpdatedDate(new Date());
        Map<String, EntityFieldValueHandler<?>> updates = new LinkedHashMap<>();
        updates.put("flycoreAlias", new SetFieldValueHandler<>(testSample.getFlycoreAlias()));
        updates.put("dataSet", new SetFieldValueHandler<>(testSample.getDataSet()));
        updates.put("line", new SetFieldValueHandler<>(testSample.getLine()));
        updates.put("effector", new SetFieldValueHandler<>(testSample.getEffector()));
        updates.put("objectiveSamples", new SetFieldValueHandler<>(testSample.getObjectiveSamples()));
        testDao.update(testSample, updates);
    }

    @Test
    public void lockAndUnlockAnUnlockedSample() {
        Sample testSample = createTestSample("ds1", "sc1");
        testDao.save(testSample);
        Sample savedSample = testDao.findById(testSample.getId());
        assertThat(savedSample.getLockKey(), nullValue());
        assertThat(savedSample.getLockTimestamp(), nullValue());
        // unlocking an unlocked sample has no effect
        assertFalse(testDao.unlockEntity("AnyKey", savedSample));
        savedSample = testDao.findById(testSample.getId());
        assertThat(savedSample.getLockKey(), nullValue());
        assertThat(savedSample.getLockTimestamp(), nullValue());
        // now place the lock
        assertTrue(testDao.lockEntity("LockKey", testSample));
        Sample savedLockedSample = testDao.findById(testSample.getId());
        assertThat(savedLockedSample.getLockKey(), equalTo("LockKey"));
        assertThat(savedLockedSample.getLockTimestamp(), not(nullValue()));
    }

    @Test
    public void lockAndUnlockAnAlreadyLockedSample() {
        Sample testSample = createTestSample("ds1", "sc1");
        testDao.save(testSample);
        assertTrue(testDao.lockEntity("LockKey", testSample));
        Sample savedLockedSample = testDao.findById(testSample.getId());
        assertThat(savedLockedSample.getLockKey(), equalTo("LockKey"));
        // I cannot place another lock on an already locked sample
        assertFalse(testDao.lockEntity("NewKey", savedLockedSample));
        savedLockedSample = testDao.findById(testSample.getId());
        assertThat(savedLockedSample.getLockKey(), equalTo("LockKey"));
        // The sample cannot be unlocked with the wrong key
        assertFalse(testDao.lockEntity("WrongKey", savedLockedSample));
        savedLockedSample = testDao.findById(testSample.getId());
        assertThat(savedLockedSample.getLockKey(), equalTo("LockKey"));
        // Now unlock it and test that the lock can be placed
        assertTrue(testDao.unlockEntity("LockKey", savedLockedSample));
        assertTrue(testDao.lockEntity("NewKey", savedLockedSample));
        savedLockedSample = testDao.findById(testSample.getId());
        assertThat(savedLockedSample.getLockKey(), equalTo("NewKey"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void updateSampleObjectivePipelineRuns() {
        Sample testSample = createTestSample("ds1", "sc1");
        testSample.getObjectiveSamples().addAll(ImmutableList.of(
                createSampleObjective("o1"),
                createSampleObjective("o2"),
                createSampleObjective("o3")));
        testDao.save(testSample);
        testDao.addObjectivePipelineRun(testSample, "o1", createPipelineRun(1, "o1.1"));
        testDao.addObjectivePipelineRun(testSample, "o1", createPipelineRun(2, "o1.2"));
        testDao.addObjectivePipelineRun(testSample, "o3", createPipelineRun(3, "o3.1"));
        testDao.addObjectivePipelineRun(testSample, "o3", createPipelineRun(4, "o3.2"));
        Sample retrievedSample = testDao.findById(testSample.getId());
        assertThat(retrievedSample.lookupObjective("o1").get().getPipelineRuns(),
                contains(new HasPropertyWithValue<>("name", equalTo("o1.1")), new HasPropertyWithValue<>("name", equalTo("o1.2")))
        );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void updateSampleObjectivePipelineResult() {
        Sample testSample = createTestSample("ds1", "sc1");
        testSample.getObjectiveSamples().addAll(ImmutableList.of(
                createSampleObjective("o1"),
                createSampleObjective("o2"),
                createSampleObjective("o3")));
        testDao.save(testSample);
        SamplePipelineRun pipelineRun = createPipelineRun(1, "o1.1");
        PipelineResult r1 = createPipelineResult("o1.1 r1");
        PipelineResult r2 = createPipelineResult("o1.1 r2");
        pipelineRun.addResult(r1);
        pipelineRun.addResult(r2);

        PipelineResult r1_1 = createPipelineResult("o1.1 r1_1");
        PipelineResult r1_2 = createPipelineResult("o1.1 r1_2");
        PipelineResult r2_1 = createPipelineResult("o1.1 r2_1");

        r1.addResult(r1_1);
        r1.addResult(r1_2);
        r2.addResult(r2_1);

        testDao.addObjectivePipelineRun(testSample, "o1", pipelineRun);

        testSample = testDao.findById(testSample.getId());
        testDao.addSampleObjectivePipelineRunResult(testSample, "o1", 1, r1_2.getId(), createPipelineResult("new o1.1. result"));
        testDao.addSampleObjectivePipelineRunResult(testSample, "o1", 1, r1.getId(), createPipelineResult("new o1.1 r1 result"));
        testDao.addSampleObjectivePipelineRunResult(testSample, "o1", 2, null, createPipelineResult("result not created"));

        Sample retrievedSample = testDao.findById(testSample.getId());
        assertThat(
                retrievedSample.lookupObjective("o1")
                        .flatMap(os -> os.findPipelineRunById(1))
                        .map(positionalRun -> positionalRun.getReference().streamResults().map(indexedResult -> indexedResult.getReference()).collect(Collectors.toList()))
                        .get(),
                contains(new HasPropertyWithValue<>("name", equalTo("o1.1")),
                        new HasPropertyWithValue<>("name", equalTo("o1.1 r1")),
                        new HasPropertyWithValue<>("name", equalTo("o1.1 r1_1")),
                        new HasPropertyWithValue<>("name", equalTo("o1.1 r1_2")),
                        new HasPropertyWithValue<>("name", equalTo("new o1.1. result")),
                        new HasPropertyWithValue<>("name", equalTo("new o1.1 r1 result")),
                        new HasPropertyWithValue<>("name", equalTo("o1.1 r2")),
                        new HasPropertyWithValue<>("name", equalTo("o1.1 r2_1"))
                )
        );
    }

    private ObjectiveSample createSampleObjective(String o) {
        ObjectiveSample so = new ObjectiveSample();
        so.setObjective(o);
        so.setChanSpec("cs");
        return so;
    }

    private SamplePipelineRun createPipelineRun(Integer runId, String s) {
        SamplePipelineRun pipelineRun = new SamplePipelineRun();
        pipelineRun.setId(runId);
        pipelineRun.setName(s);
        pipelineRun.addResult(createPipelineResult(s));
        return pipelineRun;
    }

    private PipelineResult createPipelineResult(String s) {
        PipelineResult result = new PipelineResult();
        result.setId(++nextResultId);
        result.setName(s);
        return result;
    }

    protected List<Sample> createMultipleTestItems(int nItems) {
        List<Sample> testItems = new ArrayList<>();
        for (int i = 0; i < nItems; i++) {
            testItems.add(createTestSample("ds" + (i + 1), "sc" + (i + 1)));
        }
        return testItems;
    }

    private Sample createTestSample(String dataset, String slideCode) {
        Sample testSample = new Sample();
        Date currentTime = new Date();
        testSample.setDataSet(dataset);
        testSample.setSlideCode(slideCode);
        testSample.setFlycoreAlias("testAlias");
        testSample.setCompletionDate(currentTime);
        testSample.setTmogDate(currentTime);
        testSample.setOwnerKey(TEST_OWNER_KEY);
        testSample.addObjective(createTestObjective());
        testData.add(testSample);
        return testSample;
    }

    private ObjectiveSample createTestObjective() {
        ObjectiveSample objective = new ObjectiveSample();
        objective.setObjective("testObjective");
        objective.setChanSpec("rgb");
        objective.addTiles(createTile());
        return objective;
    }

    private SampleTile createTile() {
        SampleTile sampleTile = new SampleTile();
        sampleTile.addLsmReference(Reference.createFor("LSMImage", dataGenerator.nextLong()));
        DomainModelUtils.setFullPathForFileType(sampleTile, FileType.ChanFile, "testChanFile");
        DomainModelUtils.setFullPathForFileType(sampleTile, FileType.MaskFile, "testMaskFile");
        return sampleTile;
    }

}
