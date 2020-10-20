package org.janelia.model.access.dao.mongo;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.bson.conversions.Bson;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.beans.HasPropertyWithValue;
import org.janelia.model.access.dao.JacsServiceDataDao;
import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.jacs2.SetFieldValueHandler;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.janelia.model.service.JacsServiceEvent;
import org.janelia.model.service.JacsServiceLifecycleStage;
import org.janelia.model.service.JacsServiceState;
import org.janelia.model.service.ProcessingLocation;
import org.janelia.model.service.RegisteredJacsNotification;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

public class JacsServiceDataMongoDaoITest extends AbstractMongoDaoITest<JacsServiceData> {

    private List<JacsServiceData> testData = new ArrayList<>();
    private JacsServiceDataDao testDao;

    @Before
    public void setUp() {
        testDao = new JacsServiceDataMongoDao(testMongoDatabase, idGenerator, true);
    }

    @After
    public void tearDown() {
        // delete the data that was created for testing
        deleteAll(testDao, testData);
    }

    @Test
    public void findByNullId() {
        assertNull(testDao.findById(null));
    }

    @Test
    public void findHierarchyByNullId() {
        assertNull(testDao.findServiceHierarchy(null));
    }

    @Test
    public void persistServiceData() {
        JacsServiceData si = persistServiceWithEvents(createTestService("s", ProcessingLocation.LOCAL),
                ImmutableMap.of(
                        "s1", new RegisteredJacsNotification().addNotificationField("nf1", "nv1").withDefaultLifecycleStages(),
                        "s2", new RegisteredJacsNotification().addNotificationField("nf2", "nv2").forLifecycleStage(JacsServiceLifecycleStage.FAILED_PROCESSING)
                ),
                createTestServiceEvent("e1", "v1"),
                createTestServiceEvent("e2", "v2"));
        JacsServiceData retrievedSi = testDao.findById(si.getId());
        MatcherAssert.assertThat(retrievedSi.getName(), equalTo(si.getName()));
    }

    @Test
    public void persistServiceWithTagsAndDictionaryArgs() {
        JacsServiceData serviceData = createTestService("testService", ProcessingLocation.LOCAL);
        serviceData.setTags(ImmutableList.of("t6", "t5", "t4", "t3", "t2", "t1"));
        serviceData.setDictionaryArgs(ImmutableMap.<String, Object>of(
                "a1", ImmutableMap.<String, String>of("a1.1","v1.1", "a1.2", "v1.2"),
                "a2", "v2",
                "a3", ImmutableMap.<String, Object>of("a3.1", "v3.1", "a3.2", "v3.2")
        ));
        testDao.save(serviceData);
        List<JacsServiceData> otherServices = ImmutableList.of(
                createTestService("other", ProcessingLocation.LOCAL),
                createTestService("other", ProcessingLocation.LOCAL),
                createTestService("other", ProcessingLocation.LOCAL),
                createTestService("other", ProcessingLocation.LOCAL)
        );
        otherServices.forEach(s -> {
            s.setState(JacsServiceState.QUEUED);
            testDao.save(s);
        });
        // now search by tags
        JacsServiceData pattern = new JacsServiceData();
        pattern.setState(null);
        PageResult<JacsServiceData> searchResult = testDao.findMatchingServices(pattern, new DataInterval<>(null, null), new PageRequest());
        MatcherAssert.assertThat(searchResult.getResultList(), hasSize(otherServices.size() + 1));
        MatcherAssert.assertThat(testDao.countMatchingServices(pattern, new DataInterval<>(null, null)), equalTo(otherServices.size() + 1L));

        pattern.setTags(ImmutableList.of("t1", "t3", "t2"));
        searchResult = testDao.findMatchingServices(pattern, new DataInterval<>(null, null), new PageRequest());
        MatcherAssert.assertThat(searchResult.getResultList(), hasSize(1));
        MatcherAssert.assertThat(searchResult.getResultList().get(0).getTags(), contains("t6", "t5", "t4", "t3", "t2", "t1"));
        MatcherAssert.assertThat(searchResult.getResultList().get(0).getDictionaryArgs(), Matchers.allOf(
                Matchers.<String, Object>hasEntry("a2", "v2"),
                Matchers.<String, Object>hasEntry("a1", ImmutableMap.<String, String>of("a1.1","v1.1", "a1.2", "v1.2")),
                Matchers.<String, Object>hasEntry("a3", ImmutableMap.<String, Object>of("a3.1", "v3.1", "a3.2", "v3.2"))
        ));

        pattern.setTags(ImmutableList.of("t1", "t3", "t7"));
        searchResult = testDao.findMatchingServices(pattern, new DataInterval<>(null, null), new PageRequest());
        MatcherAssert.assertThat(searchResult.getResultList(), emptyCollectionOf(JacsServiceData.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void addServiceEvent() {
        JacsServiceData si = persistServiceWithEvents(createTestService("s", ProcessingLocation.LOCAL),
                ImmutableMap.of(
                        "s1", new RegisteredJacsNotification().addNotificationField("nf1", "nv1").withDefaultLifecycleStages(),
                        "s2", new RegisteredJacsNotification().addNotificationField("nf2", "nv2").forLifecycleStage(JacsServiceLifecycleStage.FAILED_PROCESSING)
                ),
                createTestServiceEvent("e1", "v1"),
                createTestServiceEvent("e2", "v2"));
        testDao.update(si, si.addNewEvent(createTestServiceEvent("e3", "v3")));
        testDao.update(si, si.addNewEvent(createTestServiceEvent("e4", "v4")));
        testDao.update(si, ImmutableMap.of("state", new SetFieldValueHandler<>(JacsServiceState.RUNNING)));
        JacsServiceData retrievedSi = testDao.findById(si.getId());
        MatcherAssert.assertThat(retrievedSi.getName(), equalTo(si.getName()));
        MatcherAssert.assertThat(retrievedSi.getEvents(),
                contains(new HasPropertyWithValue<>("name", CoreMatchers.equalTo("e1")),
                        new HasPropertyWithValue<>("name", CoreMatchers.equalTo("e2")),
                        new HasPropertyWithValue<>("name", CoreMatchers.equalTo("e3")),
                        new HasPropertyWithValue<>("name", CoreMatchers.equalTo("e4"))
                ));
    }

    @Test
    public void persistServiceHierarchyOneAtATime() {
        JacsServiceData si1 = persistServiceWithEvents(createTestService("s1", ProcessingLocation.LOCAL),
                ImmutableMap.of(
                        "s1", new RegisteredJacsNotification().addNotificationField("nf1", "nv1"),
                        "s2", new RegisteredJacsNotification().addNotificationField("nf2", "nv2")
                ));
        JacsServiceData retrievedSi1 = testDao.findById(si1.getId());
        MatcherAssert.assertThat(retrievedSi1, allOf(
                hasProperty("parentServiceId", nullValue(Long.class)),
                hasProperty("rootServiceId", nullValue(Long.class))
        ));
        JacsServiceData si1_1 = createTestService("s1.1", ProcessingLocation.LOCAL);
        si1_1.updateParentService(si1);
        testDao.save(si1_1);
        JacsServiceData retrievedSi1_1 = testDao.findById(si1_1.getId());
        MatcherAssert.assertThat(retrievedSi1_1, allOf(
                hasProperty("parentServiceId", equalTo(si1.getId())),
                hasProperty("rootServiceId", equalTo(si1.getId()))
        ));

        JacsServiceData si1_2 = createTestService("s1.2", ProcessingLocation.LOCAL);
        si1_2.updateParentService(si1);
        testDao.save(si1_2);
        JacsServiceData retrievedSi1_2 = testDao.findById(si1_2.getId());
        MatcherAssert.assertThat(retrievedSi1_2, allOf(
                hasProperty("parentServiceId", equalTo(si1.getId())),
                hasProperty("rootServiceId", equalTo(si1.getId()))
        ));

        JacsServiceData si1_2_1 = createTestService("s1.2.1", ProcessingLocation.LOCAL);
        si1_2_1.updateParentService(si1_2);
        testDao.save(si1_2_1);

        JacsServiceData retrievedSi1_2_1 = testDao.findById(si1_2_1.getId());
        MatcherAssert.assertThat(retrievedSi1_2_1, allOf(
                hasProperty("parentServiceId", equalTo(si1_2.getId())),
                hasProperty("rootServiceId", equalTo(si1.getId()))
        ));

        List<JacsServiceData> s1Children = testDao.findChildServices(si1.getId());
        MatcherAssert.assertThat(s1Children.size(), equalTo(2));
        MatcherAssert.assertThat(s1Children, everyItem(Matchers.hasProperty("parentServiceId", equalTo(si1.getId()))));

        List<JacsServiceData> s1Hierarchy = testDao.findServiceHierarchy(si1.getId()).serviceHierarchyStream().collect(Collectors.toList());
        MatcherAssert.assertThat(s1Hierarchy.size(), equalTo(4));
        MatcherAssert.assertThat(s1Hierarchy.subList(1, s1Hierarchy.size()), everyItem(Matchers.hasProperty("rootServiceId", equalTo(si1.getId()))));
        MatcherAssert.assertThat(s1Hierarchy.get(0), Matchers.hasProperty("rootServiceId", Matchers.nullValue()));
    }

    @Test
    public void persistServiceHierarchyAllAtOnce() {
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
        testDao.saveServiceHierarchy(si1);

        List<JacsServiceData> s1Hierarchy = testDao.findServiceHierarchy(si1.getId()).serviceHierarchyStream().collect(Collectors.toList());;
        MatcherAssert.assertThat(s1Hierarchy.size(), equalTo(5));
        MatcherAssert.assertThat(s1Hierarchy.subList(1, s1Hierarchy.size()), everyItem(Matchers.hasProperty("rootServiceId", equalTo(si1.getId()))));
        MatcherAssert.assertThat(s1Hierarchy.get(0), Matchers.hasProperty("rootServiceId", Matchers.nullValue()));

        List<JacsServiceData> s1_1_Hierarchy = testDao.findServiceHierarchy(si1_1.getId()).serviceHierarchyStream().collect(Collectors.toList());;
        MatcherAssert.assertThat(s1_1_Hierarchy.size(), equalTo(4));
        MatcherAssert.assertThat(s1_1_Hierarchy, everyItem(Matchers.hasProperty("rootServiceId", equalTo(si1.getId()))));

        List<JacsServiceData> s1_2_Hierarchy = testDao.findServiceHierarchy(si1_2.getId()).serviceHierarchyStream().collect(Collectors.toList());;
        MatcherAssert.assertThat(s1_2_Hierarchy.size(), equalTo(2));
        MatcherAssert.assertThat(s1_2_Hierarchy, everyItem(Matchers.hasProperty("rootServiceId", equalTo(si1.getId()))));

        List<JacsServiceData> s1_2_1_Hierarchy = testDao.findServiceHierarchy(si1_2_1.getId()).serviceHierarchyStream().collect(Collectors.toList());;
        MatcherAssert.assertThat(s1_2_1_Hierarchy.size(), equalTo(1));
        MatcherAssert.assertThat(s1_2_1_Hierarchy, everyItem(Matchers.hasProperty("rootServiceId", equalTo(si1.getId()))));
    }

    @Test
    public void retrieveServicesByState() {
        List<JacsServiceData> servicesInQueuedState = ImmutableList.of(
                createTestService("s1.1", ProcessingLocation.LOCAL),
                createTestService("s1.2", ProcessingLocation.LOCAL),
                createTestService("s1.3", ProcessingLocation.LOCAL),
                createTestService("s1.4", ProcessingLocation.LOCAL)
        );
        List<JacsServiceData> servicesInRunningState = ImmutableList.of(
                createTestService("s2.4", ProcessingLocation.LSF_JAVA),
                createTestService("s2.5", ProcessingLocation.LSF_JAVA),
                createTestService("s2.6", ProcessingLocation.LSF_JAVA)
        );
        List<JacsServiceData> servicesInCanceledState = ImmutableList.of(
                createTestService("s7", null),
                createTestService("s8", null),
                createTestService("s9", null)
        );
        servicesInQueuedState.stream().forEach(s -> {
            s.setState(JacsServiceState.QUEUED);
            persistServiceWithEvents(s, ImmutableMap.of(
                            "s1", new RegisteredJacsNotification().addNotificationField("nf1", "nv1").withDefaultLifecycleStages(),
                            "s2", new RegisteredJacsNotification().addNotificationField("nf2", "nv2").withDefaultLifecycleStages()
                    )
            );
        });
        servicesInRunningState.stream().forEach(s -> {
            s.setState(JacsServiceState.RUNNING);
            persistServiceWithEvents(s, ImmutableMap.of());
        });
        servicesInCanceledState.stream().forEach(s -> {
            s.setState(JacsServiceState.CANCELED);
            persistServiceWithEvents(s, ImmutableMap.of(
                            "s1", new RegisteredJacsNotification().addNotificationField("nf1", "nv1").forLifecycleStage(JacsServiceLifecycleStage.START_PROCESSING)
                    )
            );
        });
        PageRequest pageRequest = new PageRequest();
        PageResult<JacsServiceData> retrievedQueuedServices = testDao.findServicesByState(ImmutableSet.of(JacsServiceState.QUEUED), pageRequest);
        MatcherAssert.assertThat(retrievedQueuedServices.getResultList(), everyItem(Matchers.hasProperty("state", equalTo(JacsServiceState.QUEUED))));
        MatcherAssert.assertThat(retrievedQueuedServices.getResultList().size(), equalTo(servicesInQueuedState.size()));

        PageResult<JacsServiceData> retrievedRunningOrCanceledServices = testDao.findServicesByState(
                ImmutableSet.of(JacsServiceState.RUNNING, JacsServiceState.CANCELED), pageRequest);
        MatcherAssert.assertThat(retrievedRunningOrCanceledServices.getResultList().size(), equalTo(servicesInRunningState.size() + servicesInCanceledState.size()));
    }

    @Test
    public void claimUnassignedFirstThenAssignedServices() {
        List<JacsServiceData> servicesInQueuedState = ImmutableList.of(
                createTestService("s1.1", ProcessingLocation.LOCAL),
                createTestService("s1.2", ProcessingLocation.LOCAL),
                createTestService("s1.3", ProcessingLocation.LOCAL),
                createTestService("s1.4", ProcessingLocation.LOCAL)
        );
        List<JacsServiceData> servicesInRunningState = ImmutableList.of(
                createTestService("s2.4", ProcessingLocation.LSF_JAVA),
                createTestService("s2.5", ProcessingLocation.LSF_JAVA),
                createTestService("s2.6", ProcessingLocation.LSF_JAVA)
        );
        List<JacsServiceData> servicesInCanceledState = ImmutableList.of(
                createTestService("s7", null),
                createTestService("s8", null),
                createTestService("s9", null)
        );
        servicesInQueuedState.stream().forEach(s -> {
            s.setState(JacsServiceState.QUEUED);
            persistServiceWithEvents(s, ImmutableMap.of());
        });
        servicesInRunningState.stream().forEach(s -> {
            s.setState(JacsServiceState.RUNNING);
            persistServiceWithEvents(s, ImmutableMap.of());
        });
        servicesInCanceledState.stream().forEach(s -> {
            s.setState(JacsServiceState.CANCELED);
            persistServiceWithEvents(s, ImmutableMap.of());
        });
        String testQueueId = "testQueueId";
        PageRequest pageRequest = new PageRequest();
        PageResult<JacsServiceData> retrievedQueuedServices;

        // claim only preassigned services - at this point there shouldn't be any
        retrievedQueuedServices = testDao.claimServiceByQueueAndState(testQueueId, true, ImmutableSet.of(JacsServiceState.QUEUED), pageRequest);
        MatcherAssert.assertThat(retrievedQueuedServices.getResultList(), emptyCollectionOf(JacsServiceData.class));
        // now actually claim unassigned services
        retrievedQueuedServices = testDao.claimServiceByQueueAndState(testQueueId, false, ImmutableSet.of(JacsServiceState.QUEUED), pageRequest);
        MatcherAssert.assertThat(retrievedQueuedServices.getResultList(), everyItem(Matchers.hasProperty("state", equalTo(JacsServiceState.QUEUED))));
        MatcherAssert.assertThat(retrievedQueuedServices.getResultList(), everyItem(Matchers.hasProperty("queueId", equalTo(testQueueId))));
        MatcherAssert.assertThat(retrievedQueuedServices.getResultList().size(), equalTo(servicesInQueuedState.size()));
        // then try to claim them for a different queue
        retrievedQueuedServices = testDao.claimServiceByQueueAndState("otherQueue", false, ImmutableSet.of(JacsServiceState.QUEUED), pageRequest);
        MatcherAssert.assertThat(retrievedQueuedServices.getResultList(), emptyCollectionOf(JacsServiceData.class));
        // claim them again for the same queue that claimed them first
        retrievedQueuedServices = testDao.claimServiceByQueueAndState(testQueueId, false, ImmutableSet.of(JacsServiceState.QUEUED), pageRequest);
        MatcherAssert.assertThat(retrievedQueuedServices.getResultList(), everyItem(Matchers.hasProperty("state", equalTo(JacsServiceState.QUEUED))));
        MatcherAssert.assertThat(retrievedQueuedServices.getResultList(), everyItem(Matchers.hasProperty("queueId", equalTo(testQueueId))));
        MatcherAssert.assertThat(retrievedQueuedServices.getResultList().size(), equalTo(servicesInQueuedState.size()));
        // test again with onlyPreAssigned true
        retrievedQueuedServices = testDao.claimServiceByQueueAndState(testQueueId, true, ImmutableSet.of(JacsServiceState.QUEUED), pageRequest);
        MatcherAssert.assertThat(retrievedQueuedServices.getResultList(), everyItem(Matchers.hasProperty("state", equalTo(JacsServiceState.QUEUED))));
        MatcherAssert.assertThat(retrievedQueuedServices.getResultList(), everyItem(Matchers.hasProperty("queueId", equalTo(testQueueId))));
        MatcherAssert.assertThat(retrievedQueuedServices.getResultList().size(), equalTo(servicesInQueuedState.size()));

        PageResult<JacsServiceData> retrievedRunningOrCanceledServices = testDao.findServicesByState(
                ImmutableSet.of(JacsServiceState.RUNNING, JacsServiceState.CANCELED), pageRequest);
        MatcherAssert.assertThat(retrievedRunningOrCanceledServices.getResultList().size(), equalTo(servicesInRunningState.size() + servicesInCanceledState.size()));
    }

    @Test
    public void simulateConcurrentClaim() {
        List<JacsServiceData> servicesInQueuedState = ImmutableList.of(
                createTestService("s1.1", ProcessingLocation.LOCAL),
                createTestService("s1.2", ProcessingLocation.LOCAL),
                createTestService("s1.3", ProcessingLocation.LOCAL),
                createTestService("s1.4", ProcessingLocation.LOCAL)
        );
        List<JacsServiceData> servicesInRunningState = ImmutableList.of(
                createTestService("s2.4", ProcessingLocation.LSF_JAVA),
                createTestService("s2.5", ProcessingLocation.LSF_JAVA),
                createTestService("s2.6", ProcessingLocation.LSF_JAVA)
        );
        List<JacsServiceData> servicesInCanceledState = ImmutableList.of(
                createTestService("s7", null),
                createTestService("s8", null),
                createTestService("s9", null)
        );
        servicesInQueuedState.stream().forEach(s -> {
            s.setState(JacsServiceState.QUEUED);
            persistServiceWithEvents(s, ImmutableMap.of());
        });
        servicesInRunningState.stream().forEach(s -> {
            s.setState(JacsServiceState.RUNNING);
            persistServiceWithEvents(s, ImmutableMap.of());
        });
        servicesInCanceledState.stream().forEach(s -> {
            s.setState(JacsServiceState.CANCELED);
            persistServiceWithEvents(s, ImmutableMap.of());
        });
        String testQueueId = "testQueueId";
        PageRequest pageRequest = new PageRequest();

        JacsServiceDataMongoDao spiedTestDao = new JacsServiceDataMongoDao(testMongoDatabase, idGenerator, true) {
            @Override
            protected Class<JacsServiceData> getEntityType() {
                return JacsServiceData.class;
            }

            @Override
            protected <R> List<R> find(Bson queryFilter, Bson sortCriteria, long offset, int length, Class<R> resultType) {
                List<R> candidates = super.find(queryFilter, sortCriteria, offset, length, resultType);
                // while one queue is trying to claim the services some other queue attempts to do the same
                // and the other queue manages to get the services first
                candidates.stream().forEach(e -> {
                    JacsServiceData sd = (JacsServiceData) e;
                    mongoCollection.findOneAndUpdate(
                            Filters.and(Filters.eq("_id", sd.getId())),
                            Updates.combine(
                                    Updates.set("queueId", "otherqueue")
                            )
                    );
                });
                return candidates;
            }
        };
        PageResult<JacsServiceData> retrievedQueuedServices = spiedTestDao.claimServiceByQueueAndState(testQueueId, false, ImmutableSet.of(JacsServiceState.QUEUED), pageRequest);
        MatcherAssert.assertThat(retrievedQueuedServices.getResultList(), emptyCollectionOf(JacsServiceData.class));
    }

    @Test
    public void searchServicesByUserStateAndDateRange() {
        Calendar testCal = Calendar.getInstance();
        testCal.add(Calendar.DATE, -100);
        Date startDate = testCal.getTime();
        testCal.add(Calendar.DATE, 1);
        List<JacsServiceData> testServices = persistServicesForSearchTest(testCal);
        testCal.add(Calendar.DATE, 1);
        Date endDate = testCal.getTime();

        JacsServiceData emptyRequest = new JacsServiceData();
        PageRequest pageRequest = new PageRequest();
        PageResult<JacsServiceData> retrievedQueuedServices;

        retrievedQueuedServices = testDao.findMatchingServices(emptyRequest, new DataInterval<>(null, null), pageRequest);
        MatcherAssert.assertThat(retrievedQueuedServices.getResultList(), everyItem(Matchers.hasProperty("id", Matchers.in(testServices.stream().map(e->e.getId()).toArray()))));

        JacsServiceData u1ServicesRequest = new JacsServiceData();
        u1ServicesRequest.setOwnerKey("user:u1");
        u1ServicesRequest.setState(JacsServiceState.QUEUED);

        retrievedQueuedServices = testDao.findMatchingServices(u1ServicesRequest, new DataInterval<>(null, null), pageRequest);
        MatcherAssert.assertThat(retrievedQueuedServices.getResultList(), everyItem(Matchers.hasProperty("state", equalTo(JacsServiceState.QUEUED))));
        MatcherAssert.assertThat(retrievedQueuedServices.getResultList(), everyItem(Matchers.hasProperty("ownerKey", equalTo("user:u1"))));

        retrievedQueuedServices = testDao.findMatchingServices(u1ServicesRequest, new DataInterval<>(startDate, endDate), pageRequest);
        MatcherAssert.assertThat(retrievedQueuedServices.getResultList(), everyItem(Matchers.hasProperty("state", equalTo(JacsServiceState.QUEUED))));
        MatcherAssert.assertThat(retrievedQueuedServices.getResultList(), everyItem(Matchers.hasProperty("ownerKey", equalTo("user:u1"))));

        retrievedQueuedServices = testDao.findMatchingServices(u1ServicesRequest, new DataInterval<>(null, startDate), pageRequest);
        MatcherAssert.assertThat(retrievedQueuedServices.getResultList(), hasSize(0));

        retrievedQueuedServices = testDao.findMatchingServices(u1ServicesRequest, new DataInterval<>(endDate, null), pageRequest);
        MatcherAssert.assertThat(retrievedQueuedServices.getResultList(), hasSize(0));
    }

    @Test
    public void searchServicesByServiceArgs() {
        List<JacsServiceData> slist = ImmutableList.of(
                createTestService("s1", ProcessingLocation.LOCAL,
                        ImmutableMap.of("arg1", "v1.1", "arg2", "v2.1"), ImmutableMap.of("k1.sk1", "v1")),
                createTestService("s2", ProcessingLocation.LOCAL,
                        ImmutableMap.of("arg1", "v1.2", "arg2", "v2.2"), ImmutableMap.of("k1.sk1", "v1")),
                createTestService("s3", ProcessingLocation.LOCAL,
                        ImmutableMap.of("arg1", "v1.1", "arg2", "v2.2"), ImmutableMap.of("k1.sk1", "v1")),
                createTestService("s4", ProcessingLocation.LOCAL,
                        ImmutableMap.of("arg1", "v1.1", "arg2", "v2.3"), ImmutableMap.of("k1.sk1", "v1")),
                createTestService("s5", ProcessingLocation.LOCAL,
                        ImmutableMap.of("arg1", "v1.3", "arg3", 5), ImmutableMap.of("k1.sk1", "v1")),
                createTestService("s6", ProcessingLocation.LOCAL,
                        ImmutableMap.of("arg1", "v1.3", "arg3", 6), ImmutableMap.of("k1.sk1", "v1")),
                createTestService("s7", ProcessingLocation.LOCAL,
                        ImmutableMap.of("arg1", "v1.3", "arg3", 7), ImmutableMap.of("k1.sk1", "v1"))
        );
        slist.forEach(testDao::save);

        @SuppressWarnings("unchecked")
        Map<JacsServiceData, Matcher<Iterable<?>>> testData = ImmutableMap.<JacsServiceData, Matcher<Iterable<?>>>of(
                new JacsServiceDataBuilder(null)
                        .addServiceArg("arg1", "v1.1")
                        .addServiceArg("arg2", ImmutableList.of("v2.1", "v2.2"))
                        .build(),
                contains(new HasPropertyWithValue<>("name", CoreMatchers.equalTo("s1")),
                        new HasPropertyWithValue<>("name", CoreMatchers.equalTo("s3"))
                ),
                new JacsServiceDataBuilder(null)
                        .addServiceArg("arg1", "v1.2")
                        .addServiceArg("arg2", "v2.1")
                        .build(),
                equalTo(Collections.emptyList()),
                new JacsServiceDataBuilder(null)
                        .addServiceArg("arg1", "v1.1")
                        .addServiceArg("arg2", ImmutableList.of("v2.3"))
                        .build(),
                contains(new HasPropertyWithValue<>("name", CoreMatchers.equalTo("s4"))),
                new JacsServiceDataBuilder(null)
                        .addServiceArg("arg1", "v1.3")
                        .addServiceArg("arg3", new DataInterval<>(15, 17))
                        .build(),
                equalTo(Collections.emptyList()),
                new JacsServiceDataBuilder(null)
                        .addServiceArg("arg1", "v1.3")
                        .addServiceArg("arg3", new DataInterval<>(5, 7))
                        .build(),
                contains(new HasPropertyWithValue<>("name", CoreMatchers.equalTo("s5")),
                        new HasPropertyWithValue<>("name", CoreMatchers.equalTo("s6")),
                        new HasPropertyWithValue<>("name", CoreMatchers.equalTo("s7"))
                )
        );

        testData.forEach((sd, matcher) -> {
            PageRequest pageRequest = new PageRequest();
            PageResult<JacsServiceData> retrievedServices = testDao.findMatchingServices(sd, new DataInterval<>(null, null), pageRequest);
            MatcherAssert.assertThat(retrievedServices.getResultList(), matcher);
        });


    }

    @Test
    public void updateServiceHierarchy() {
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
        testDao.saveServiceHierarchy(si1);
        List<JacsServiceData> s1Hierarchy = si1.serviceHierarchyStream().collect(Collectors.toList());

        // update the state of all services from the invocation tree
        s1Hierarchy.forEach(sd -> {
            sd.setState(JacsServiceState.SUCCESSFUL);
        });

        // persist the entire tree
        testDao.saveServiceHierarchy(si1);

        s1Hierarchy.forEach(sd -> {
            JacsServiceData archivedServiceData = testDao.findById(sd.getId());
            assertNotNull(archivedServiceData);
            assertEquals(JacsServiceState.SUCCESSFUL, archivedServiceData.getState());
        });

        JacsServiceData archivedService = testDao.findServiceHierarchy(si1.getId());
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
            assertEquals(JacsServiceState.SUCCESSFUL, activeArchivedPair.getRight().getState());
        });
    }

    private List<JacsServiceData> persistServicesForSearchTest(Calendar calDate) {
        List<JacsServiceData> testServices = new ArrayList<>();
        List<JacsServiceData> u1Services = ImmutableList.of(
                createTestService("s1.1", ProcessingLocation.LOCAL),
                createTestService("s1.2", ProcessingLocation.LOCAL),
                createTestService("s1.3", ProcessingLocation.LOCAL),
                createTestService("s1.4", ProcessingLocation.LOCAL)
        );
        List<JacsServiceData> u2Services = ImmutableList.of(
                createTestService("s2.1", ProcessingLocation.LSF_JAVA),
                createTestService("s2.2", ProcessingLocation.LSF_JAVA),
                createTestService("s2.3", ProcessingLocation.LSF_JAVA)
        );
        u1Services.forEach(s -> {
            s.setOwnerKey("user:u1");
            s.setState(JacsServiceState.QUEUED);
            s.setCreationDate(calDate.getTime());
            calDate.add(Calendar.DATE, 1);
            persistServiceWithEvents(s, ImmutableMap.of());
            testServices.add(s);
        });
        u2Services.forEach(s -> {
            s.setOwnerKey("group:u2");
            s.setState(JacsServiceState.RUNNING);
            s.setCreationDate(calDate.getTime());
            calDate.add(Calendar.DATE, 1);
            persistServiceWithEvents(s, ImmutableMap.of());
            testServices.add(s);
        });
        return testServices;
    }

    private JacsServiceData persistServiceWithEvents(JacsServiceData si, Map<String, RegisteredJacsNotification> processingStageNotifications, JacsServiceEvent... jacsServiceEvents) {
        for (JacsServiceEvent se : jacsServiceEvents) {
            si.addNewEvent(se);
        }
        processingStageNotifications.forEach(si::setProcessingStageNotification);
        testDao.save(si);
        return si;
    }

    @Override
    protected List<JacsServiceData> createMultipleTestItems(int nItems) {
        List<JacsServiceData> testItems = new ArrayList<>();
        for (int i = 0; i < nItems; i++) {
            testItems.add(createTestService("s" + (i + 1), i % 2 == 0 ? ProcessingLocation.LOCAL : ProcessingLocation.LSF_JAVA));
        }
        return testItems;
    }

    private JacsServiceData createTestService(String serviceName, ProcessingLocation processingLocation) {
        return createTestService(serviceName, processingLocation, ImmutableMap.of("a1", "I1", "a2", "I2"), ImmutableMap.of("key.subKey1.subKey2", "val1"));
    }

    private JacsServiceData createTestService(String serviceName,
                                              ProcessingLocation processingLocation,
                                              Map<String, Object> serviceArgs,
                                              Map<String, String> serviceResources) {
        JacsServiceData si = new JacsServiceData();
        si.setName(serviceName);
        si.setProcessingLocation(processingLocation);
        serviceArgs.forEach((n, v) -> {
            si.addArg(n);
            si.addArg(v.toString());
            si.addServiceArg(n, v);
        });
        serviceResources.forEach((k, v) -> si.addToResources(k, v));
        testData.add(si);
        return si;
    }

    private JacsServiceEvent createTestServiceEvent(String name, String value) {
        JacsServiceEvent se = new JacsServiceEvent();
        se.setName(name);
        se.setValue(value);
        return se;
    }

}
