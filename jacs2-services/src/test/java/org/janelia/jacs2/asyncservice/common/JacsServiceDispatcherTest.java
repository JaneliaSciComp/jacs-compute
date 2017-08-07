package org.janelia.jacs2.asyncservice.common;

import com.google.common.collect.ImmutableList;
import org.janelia.jacs2.asyncservice.JacsServiceEngine;
import org.janelia.jacs2.dao.JacsNotificationDao;
import org.janelia.jacs2.model.jacsservice.JacsNotification;
import org.janelia.jacs2.model.jacsservice.JacsServiceEvent;
import org.janelia.jacs2.model.jacsservice.JacsServiceEventTypes;
import org.janelia.jacs2.model.jacsservice.JacsServiceLifecycleStage;
import org.janelia.jacs2.model.jacsservice.RegisteredJacsNotification;
import org.janelia.jacs2.model.page.PageRequest;
import org.janelia.jacs2.model.page.PageResult;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.asyncservice.ServiceRegistry;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;

import javax.enterprise.inject.Instance;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JacsServiceDispatcherTest {

    private static final Long TEST_ID = 101L;

    private ServiceComputationFactory serviceComputationFactory;
    private JacsServiceDataPersistence jacsServiceDataPersistence;
    private JacsNotificationDao jacsNotificationDao;
    private JacsServiceQueue jacsServiceQueue;
    private JacsServiceEngine jacsServiceEngine;
    private Instance<ServiceRegistry> serviceRegistrarSource;
    private ServiceRegistry serviceRegistry;
    private Logger logger;
    private JacsServiceDispatcher testDispatcher;

    @Before
    public void setUp() {
        logger = mock(Logger.class);
        serviceComputationFactory = ComputationTestUtils.createTestServiceComputationFactory(logger);

        jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        jacsNotificationDao = mock(JacsNotificationDao.class);
        serviceRegistrarSource = mock(Instance.class);
        serviceRegistry = mock(ServiceRegistry.class);
        jacsServiceQueue = new InMemoryJacsServiceQueue(jacsServiceDataPersistence, "queue", 10, logger);
        jacsServiceEngine = new JacsServiceEngineImpl(jacsServiceDataPersistence, jacsServiceQueue, serviceRegistrarSource, 10, logger);
        testDispatcher = new JacsServiceDispatcher(serviceComputationFactory,
                jacsServiceQueue,
                jacsServiceDataPersistence,
                jacsNotificationDao,
                jacsServiceEngine,
                logger);
        when(serviceRegistrarSource.get()).thenReturn(serviceRegistry);
        Answer<Void> saveServiceData = invocation -> {
            JacsServiceData ti = invocation.getArgument(0);
            ti.setId(TEST_ID);
            return null;
        };
        doAnswer(saveServiceData).when(jacsServiceDataPersistence).saveHierarchy(any(JacsServiceData.class));
        doAnswer(invocation -> {
            JacsServiceData sd = invocation.getArgument(0);
            JacsServiceState state = invocation.getArgument(1);
            sd.setState(state);
            return null;
        }).when(jacsServiceDataPersistence).updateServiceState(any(JacsServiceData.class), any(JacsServiceState.class), any(Optional.class));

    }

    @Test
    public void serviceAsyncSubmit() {
        JacsServiceData serviceData = enqueueTestService("test");

        assertThat(serviceData.getId(), equalTo(TEST_ID));
    }

    private JacsServiceData createTestService(Long serviceId, String serviceName, RegisteredJacsNotification processingNotification) {
        JacsServiceData testService = new JacsServiceData();
        testService.setId(serviceId);
        testService.setName(serviceName);
        testService.setProcessingNotification(processingNotification);
        return testService;
    }

    private JacsServiceData enqueueTestService(String serviceName) {
        JacsServiceData testService = createTestService(TEST_ID, serviceName, new RegisteredJacsNotification().withDefaultLifecycleStages().addNotificationField("f1", "v1"));
        return jacsServiceQueue.enqueueService(testService);
    }

    @Test
    public void dispatchServiceWhenNoSlotsAreAvailable() {
        jacsServiceEngine.setProcessingSlotsCount(0);
        JacsServiceData testService = enqueueTestService("test");
        when(jacsServiceDataPersistence.claimServiceByQueueAndState(anyString(), any(Set.class), any(PageRequest.class)))
                .thenReturn(new PageResult<>());
        testDispatcher.dispatchServices();
        verify(logger).info("Abort service {} for now because there are not enough processing slots", testService);
    }

    @Test
    public void runSubmittedService() {
        JacsServiceData testServiceData = enqueueTestService("submittedService");

        when(jacsServiceDataPersistence.claimServiceByQueueAndState(anyString(), any(Set.class), any(PageRequest.class)))
                .thenReturn(new PageResult<>());

        verifyDispatch(testServiceData);
    }

    @Test
    public void runServiceFromPersistenceStore() {
        JacsServiceData testServiceData = createTestService(1L, "persistedService", new RegisteredJacsNotification().withDefaultLifecycleStages().addNotificationField("f1", "v1"));

        PageResult<JacsServiceData> nonEmptyPageResult = new PageResult<>();
        nonEmptyPageResult.setResultList(ImmutableList.of(testServiceData));
        when(jacsServiceDataPersistence.claimServiceByQueueAndState(anyString(), any(Set.class), any(PageRequest.class)))
                .thenReturn(nonEmptyPageResult)
                .thenReturn(new PageResult<>());

        verifyDispatch(testServiceData);
    }

    private static class ServiceSyncer implements Runnable {
        volatile boolean done = false;
        @Override
        public void run() {
            while (!done) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    private void verifyDispatch(JacsServiceData testServiceData) {
        ServiceProcessor testProcessor = prepareServiceProcessor(testServiceData, null);

        testDispatcher.dispatchServices();
        verify(logger).debug("Dequeued service {}", testServiceData);
        ArgumentCaptor<JacsServiceData> jacsServiceArg = ArgumentCaptor.forClass(JacsServiceData.class);
        verify(testProcessor).process(jacsServiceArg.capture());
        assertSame(testServiceData, jacsServiceArg.getValue());

        verify(jacsServiceDataPersistence)
                .updateServiceState(
                        testServiceData,
                        JacsServiceState.QUEUED,
                        Optional.empty());
        verify(jacsServiceDataPersistence)
                .updateServiceState(
                        testServiceData,
                        JacsServiceState.RUNNING,
                        Optional.empty());
        verify(jacsServiceDataPersistence)
                .updateServiceState(
                        same(testServiceData),
                        eq(JacsServiceState.SUCCESSFUL),
                        argThat(new ArgumentMatcher<Optional<JacsServiceEvent>>() {
                            @Override
                            public boolean matches(Optional<JacsServiceEvent> argument) {
                                return argument.isPresent()
                                        && argument.get().getName().equals(JacsServiceEventTypes.COMPLETED.name())
                                        && argument.get().getValue().equals("Completed successfully");
                            }
                        })
                );
        verify(jacsNotificationDao)
                .save(argThat(new ArgumentMatcher<JacsNotification>() {
                    @Override
                    public boolean matches(JacsNotification argument) {
                        return argument.getNotificationStage() == JacsServiceLifecycleStage.START_PROCESSING;
                    }
                }));
        verify(jacsNotificationDao)
                .save(argThat(new ArgumentMatcher<JacsNotification>() {
                    @Override
                    public boolean matches(JacsNotification argument) {
                        return argument.getNotificationStage() == JacsServiceLifecycleStage.SUCCESSFUL_PROCESSING;
                    }
                }));
        verify(jacsNotificationDao, never())
                .save(argThat(new ArgumentMatcher<JacsNotification>() {
                    @Override
                    public boolean matches(JacsNotification argument) {
                        return argument.getNotificationStage() == JacsServiceLifecycleStage.FAILED_PROCESSING;
                    }
                }));
        assertThat(testServiceData.getState(), equalTo(JacsServiceState.SUCCESSFUL));
    }

    private ServiceProcessor prepareServiceProcessor(JacsServiceData testServiceData, Exception exc) {
        ServiceProcessor testProcessor = mock(ServiceProcessor.class);

        when(jacsServiceDataPersistence.findById(any(Number.class))).then(invocation -> testServiceData);
        when(jacsServiceDataPersistence.findServiceDependencies(any(JacsServiceData.class))).thenReturn(ImmutableList.of());
        when(serviceRegistry.lookupService(testServiceData.getName())).thenReturn(testProcessor);

        if (exc == null) {
            when(testProcessor.process(any(JacsServiceData.class))).thenAnswer(invocation -> serviceComputationFactory.newCompletedComputation(new JacsServiceResult<>(testServiceData)));
        } else {
            when(testProcessor.process(any(JacsServiceData.class))).thenAnswer(invocation -> serviceComputationFactory.newFailedComputation(exc));
        }
        return testProcessor;
    }

    @Test
    public void serviceProcessingError() {
        JacsServiceData testServiceData = enqueueTestService("submittedService");
        when(jacsServiceDataPersistence.claimServiceByQueueAndState(anyString(), any(Set.class), any(PageRequest.class)))
                .thenReturn(new PageResult<>());
        ComputationException processException = new ComputationException(testServiceData, "test exception");
        ServiceProcessor testProcessor = prepareServiceProcessor(testServiceData, processException);

        testDispatcher.dispatchServices();
        verify(logger).debug("Dequeued service {}", testServiceData);

        ArgumentCaptor<JacsServiceData> jacsServiceArg = ArgumentCaptor.forClass(JacsServiceData.class);
        verify(testProcessor).process(jacsServiceArg.capture());
        assertSame(testServiceData, jacsServiceArg.getValue());

        verify(jacsServiceDataPersistence)
                .updateServiceState(
                        testServiceData,
                        JacsServiceState.QUEUED,
                        Optional.empty());
        verify(jacsServiceDataPersistence)
                .updateServiceState(
                        testServiceData,
                        JacsServiceState.RUNNING,
                        Optional.empty());
        verify(jacsServiceDataPersistence)
                .updateServiceState(
                        same(testServiceData),
                        eq(JacsServiceState.ERROR),
                        argThat(new ArgumentMatcher<Optional<JacsServiceEvent>>() {
                            @Override
                            public boolean matches(Optional<JacsServiceEvent> argument) {
                                return argument.isPresent()
                                        && argument.get().getName().equals(JacsServiceEventTypes.FAILED.name())
                                        && argument.get().getValue().equals("Failed: test exception");
                            }
                        })
                );
        verify(jacsNotificationDao)
                .save(argThat(new ArgumentMatcher<JacsNotification>() {
                    @Override
                    public boolean matches(JacsNotification argument) {
                        return argument.getNotificationStage() == JacsServiceLifecycleStage.START_PROCESSING;
                    }
                }));
        verify(jacsNotificationDao, never())
                .save(argThat(new ArgumentMatcher<JacsNotification>() {
                    @Override
                    public boolean matches(JacsNotification argument) {
                        return argument.getNotificationStage() == JacsServiceLifecycleStage.SUCCESSFUL_PROCESSING;
                    }
                }));
        verify(jacsNotificationDao)
                .save(argThat(new ArgumentMatcher<JacsNotification>() {
                    @Override
                    public boolean matches(JacsNotification argument) {
                        return argument.getNotificationStage() == JacsServiceLifecycleStage.FAILED_PROCESSING;
                    }
                }));
        assertThat(testServiceData.getState(), equalTo(JacsServiceState.ERROR));
    }

}
