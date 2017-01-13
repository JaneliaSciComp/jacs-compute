package org.janelia.jacs2.service.impl;

import com.google.common.collect.ImmutableMap;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.model.service.JacsServiceData;
import org.janelia.jacs2.persistence.JacsServiceDataPersistence;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class AbstractExeBasedServiceProcessorTest {

    private static final String TEST_WORKING_DIR = "testDir";
    private static final String TEST_EXE_DIR = "testToolsDir";

    private static class TestExternalProcessor extends AbstractExeBasedServiceProcessor<Void> {

        public TestExternalProcessor(JacsServiceDispatcher jacsServiceDispatcher,
                                     ServiceComputationFactory computationFactory,
                                     JacsServiceDataPersistence jacsServiceDataPersistence,
                                     Instance<ExternalProcessRunner> serviceRunners,
                                     String defaultWorkingDir,
                                     String executablesBaseDir,
                                     Logger logger) {
            super(jacsServiceDispatcher, computationFactory, jacsServiceDataPersistence, defaultWorkingDir, executablesBaseDir, serviceRunners, logger);
        }

        @Override
        protected List<String> prepareCmdArgs(JacsServiceData jacsServiceData) {
            return Collections.emptyList();
        }

        @Override
        protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
            return Collections.emptyMap();
        }

        @Override
        protected ServiceComputation<Void> localProcessData(Object preprocessingResults, JacsServiceData jacsServiceData) {
            return null;
        }

        @Override
        public Void getResult(JacsServiceData jacsServiceData) {
            return null;
        }

        @Override
        public void setResult(Void result, JacsServiceData jacsServiceData) {

        }
    }

    private AbstractExeBasedServiceProcessor<?> testProcessor;

    @Before
    public void setUp() {
        JacsServiceDispatcher jacsServiceDispatcher = mock(JacsServiceDispatcher.class);
        ServiceComputationFactory serviceComputationFactory = mock(ServiceComputationFactory.class);
        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        Instance<ExternalProcessRunner> serviceRunners = mock(Instance.class);
        Logger logger = mock(Logger.class);
        testProcessor = new TestExternalProcessor(
                            jacsServiceDispatcher,
                            serviceComputationFactory,
                            jacsServiceDataPersistence,
                            serviceRunners,
                            TEST_WORKING_DIR,
                            TEST_EXE_DIR,
                            logger);
    }

    @Test
    public void checkOutputErrors() {
        ImmutableMap<String, Boolean> testData =
                new ImmutableMap.Builder<String, Boolean>()
                        .put("This has an error here", true)
                        .put("This has an ERROR here", true)
                        .put("This has an exception here", true)
                        .put("No Exception", true)
                        .put("OK here", false)
                        .put("\n", false)
                        .build();
        testData.forEach((l, r) -> assertThat(testProcessor.checkForErrors(l), equalTo(r)));
    }

}
