package org.janelia.jacs2.asyncservice.common;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.janelia.jacs2.cdi.ApplicationConfigProvider;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ProcessingLocation;
import org.janelia.model.service.ServiceMetaData;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import jakarta.enterprise.inject.Instance;
import java.nio.file.Path;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AbstractExeBasedServiceProcessorTest {

    private static final String TEST_WORKING_DIR = "testDir";
    private static final String TEST_EXE_DIR = "testToolsDir";
    private static final Long TEST_SERVICE_ID = 1L;

    private static class TestExternalProcessor extends AbstractExeBasedServiceProcessor<Void> {

        public TestExternalProcessor(ServiceComputationFactory computationFactory,
                                     JacsServiceDataPersistence jacsServiceDataPersistence,
                                     Instance<ExternalProcessRunner> serviceRunners,
                                     String defaultWorkingDir,
                                     JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                                     ApplicationConfig applicationConfig,
                                     Logger logger) {
            super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        }

        @Override
        public ServiceMetaData getMetadata() {
            return new ServiceMetaData();
        }

        @Override
        protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
            return new ExternalCodeBlock();
        }
    }

    private JacsServiceDataPersistence jacsServiceDataPersistence;
    private AbstractExeBasedServiceProcessor<Void> testProcessor;
    private ExternalProcessRunner processRunner;

    @Before
    public void setUp() {
        Logger logger = mock(Logger.class);
        ServiceComputationFactory serviceComputationFactory = ComputationTestHelper.createTestServiceComputationFactory(logger);
        jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        JacsJobInstanceInfoDao jacsJobInstanceInfoDao = mock(JacsJobInstanceInfoDao.class);
        @SuppressWarnings("unchecked")
        Instance<ExternalProcessRunner> serviceRunners = mock(Instance.class);
        processRunner = mock(ExternalProcessRunner.class);
        when(processRunner.supports(ProcessingLocation.LOCAL)).thenReturn(true);
        when(serviceRunners.iterator()).thenReturn(ImmutableList.of(processRunner).iterator());
        ApplicationConfig applicationConfig = new ApplicationConfigProvider().fromMap(
                ImmutableMap.of("Executables.ModuleBase", TEST_EXE_DIR))
                .build();
        testProcessor = new TestExternalProcessor(
                            serviceComputationFactory,
                            jacsServiceDataPersistence,
                            serviceRunners,
                            TEST_WORKING_DIR,
                            jacsJobInstanceInfoDao,
                            applicationConfig,
                            logger);
    }

    @Test
    public void processing() {
        JacsServiceData testServiceData = new JacsServiceData();
        testServiceData.setId(TEST_SERVICE_ID);
        testServiceData.setName("test");
        testServiceData.setProcessingLocation(ProcessingLocation.LOCAL);
        ExeJobHandler jobInfo = mock(ExeJobHandler.class);
        when(jobInfo.isDone()).thenReturn(true);
        when(processRunner.runCmds(any(ExternalCodeBlock.class), anyList(), anyMap(), any(JacsServiceFolder.class), any(Path.class), any(JacsServiceData.class))).thenReturn(jobInfo);
        when(jacsServiceDataPersistence.findById(TEST_SERVICE_ID)).thenReturn(testServiceData);
        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        testProcessor.process(testServiceData)
            .whenComplete((r, e) -> {
                if (e == null) {
                    successful.accept(r);
                } else {
                    failure.accept(e);
                }
            });
        verify(failure, never()).accept(any());
        verify(successful).accept(any());
    }

    @Test
    public void processingError() {
        JacsServiceData testServiceData = new JacsServiceData();
        testServiceData.setId(TEST_SERVICE_ID);
        testServiceData.setName("test");
        testServiceData.setServiceTimeout(10L);
        testServiceData.setProcessingLocation(ProcessingLocation.LOCAL);
        ExeJobHandler jobInfo = mock(ExeJobHandler.class);
        when(jobInfo.isDone()).thenReturn(true);
        when(jobInfo.hasFailed()).thenReturn(true);
        when(processRunner.runCmds(any(ExternalCodeBlock.class), anyList(), anyMap(), any(JacsServiceFolder.class), any(Path.class), any(JacsServiceData.class))).thenReturn(jobInfo);
        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        testProcessor.process(testServiceData)
                .whenComplete((r, e) -> {
                    if (e == null) {
                        successful.accept(r);
                    } else {
                        failure.accept(e);
                    }
                });
        verify(failure).accept(any());
        verify(successful, never()).accept(any());
    }

    @Test
    public void processingTimeout() {
        JacsServiceData testServiceData = new JacsServiceData();
        testServiceData.setId(TEST_SERVICE_ID);
        testServiceData.setName("test");
        testServiceData.setServiceTimeout(10L);
        testServiceData.setProcessingLocation(ProcessingLocation.LOCAL);
        ExeJobHandler jobInfo = mock(ExeJobHandler.class);
        when(jobInfo.isDone()).thenReturn(false);
        when(processRunner.runCmds(any(ExternalCodeBlock.class), anyList(), anyMap(), any(JacsServiceFolder.class), any(Path.class), any(JacsServiceData.class)))
                .thenReturn(jobInfo);
        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        testProcessor.process(testServiceData)
                .whenComplete((r, e) -> {
                    if (e == null) {
                        successful.accept(r);
                    } else {
                        failure.accept(e);
                    }
                });
        verify(failure).accept(any());
        verify(successful, never()).accept(any());
    }
}
