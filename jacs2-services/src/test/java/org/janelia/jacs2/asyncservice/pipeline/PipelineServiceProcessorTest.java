package org.janelia.jacs2.asyncservice.pipeline;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.janelia.jacs2.asyncservice.common.ComputationTestHelper;
import org.janelia.jacs2.asyncservice.common.GenericAsyncServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgMatcher;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceProcessorTestHelper;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.containerizedservices.PullSingularityContainerProcessor;
import org.janelia.jacs2.asyncservice.containerizedservices.SimpleRunSingularityContainerProcessor;
import org.janelia.jacs2.asyncservice.lightsheetservices.LightsheetPipelineStep;
import org.janelia.jacs2.asyncservice.lightsheetservices.LightsheetPipelineStepProcessor;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.ApplicationConfigProvider;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.testhelpers.ListArgMatcher;
import org.janelia.jacs2.utils.HttpUtils;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.janelia.model.service.JacsServiceState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class PipelineServiceProcessorTest {

    private static final String TEST_WORKING_DIR = "testDir";
    private static volatile long testServiceId = 21L;

    private Map<Number, JacsServiceData> serviceStorage = new HashMap<>();
    private PipelineServiceProcessor pipelineServiceProcessor;
    private GenericAsyncServiceProcessor genericAsyncServiceProcessor;

    @Before
    public void setUp() {
        Logger logger = mock(Logger.class);
        ServiceComputationFactory serviceComputationFactory = ComputationTestHelper.createTestServiceComputationFactory(logger);
        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        genericAsyncServiceProcessor = spy(new GenericAsyncServiceProcessor(serviceComputationFactory,
                jacsServiceDataPersistence,
                TEST_WORKING_DIR,
                logger));

        Mockito.when(jacsServiceDataPersistence.findById(any(Number.class))).then(invocation -> {
            JacsServiceData sd = serviceStorage.get(invocation.getArgument(0));
            if (sd != null) {
                sd.setState(JacsServiceState.SUCCESSFUL);
            }
            return sd;
        });
        Mockito.when(jacsServiceDataPersistence.createServiceIfNotFound(any(JacsServiceData.class))).then(invocation -> {
            JacsServiceData jacsServiceData = invocation.getArgument(0);
            jacsServiceData.setId(++testServiceId);
            serviceStorage.put(jacsServiceData.getId(), jacsServiceData);
            return jacsServiceData;
        });

        pipelineServiceProcessor = new PipelineServiceProcessor(serviceComputationFactory,
                jacsServiceDataPersistence,
                TEST_WORKING_DIR,
                genericAsyncServiceProcessor,
                logger);
    }

    @Test
    public void processPipeline() {
        JacsServiceData testServiceData = createTestService(createPipelineConfig());
        ServiceComputation<JacsServiceResult<Void>> pipelineComputation = pipelineServiceProcessor.process(testServiceData);
        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        pipelineComputation
                .thenApply(r -> {
                    successful.accept(r);
                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    fail(exc.toString());
                    return null;
                });
        Mockito.verify(successful).accept(any());
        Mockito.verify(failure, never()).accept(any());
    }

    private JacsServiceData createTestService(Map<String, Object> dictionaryArgs) {
        JacsServiceData testServiceData = new JacsServiceDataBuilder(null)
                .setDictionaryArgs(dictionaryArgs)
                .build();
        testServiceData.setId(testServiceId);
        return testServiceData;
    }

    private Map<String, Object> createPipelineConfig() {
        return ImmutableMap.of(
                "pipelineConfig",
                ImmutableMap.<String, Object>builder()
                        .put("pipelineServices",
                                ImmutableList.of(
                                        createServiceConfig("s1"),
                                        createServiceConfig("s2")))
                .build()
        );
    }

    private Map<String, Object> createServiceConfig(String serviceName) {
        return ImmutableMap.<String, Object>builder()
                .put("serviceName", serviceName)
                .put("serviceArgs", ImmutableList.of("-f1", "v1", "-f2", "'v2.1,v2.2,v2.3'", "-f3", "v3"))
                .put("serviceResources", ImmutableMap.of("r1", "rv1", "r2", "rv2", "r3", "rv3"))
                .put("serviceKeyArgs", ImmutableMap.of("k1", "kv1", "k2", "kv2", "k3", "kv3"))
                .build();
    }
}
