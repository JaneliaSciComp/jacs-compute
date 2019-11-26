package org.janelia.jacs2.asyncservice.pipeline;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.ComputationTestHelper;
import org.janelia.jacs2.asyncservice.common.GenericAsyncServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgMatcher;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.janelia.model.service.JacsServiceState;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;

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
        JacsServiceData testServiceData = createTestService(null, createPipelineConfig(null));
        ServiceComputation<JacsServiceResult<Void>> pipelineComputation = pipelineServiceProcessor.process(testServiceData);
        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        pipelineComputation
                .thenApply(r -> {
                    successful.accept(r);
                    Mockito.verify(genericAsyncServiceProcessor).process(
                            any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceName", "s1"))),
                            argThat(new ServiceArgMatcher(new ServiceArg(Arrays.asList("-f1", "v1", "-f2", "'v2.1,v2.2,v2.3'", "-f3", "v3.1, v3.2"))))
                    );
                    Mockito.verify(genericAsyncServiceProcessor).process(
                            any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceName", "s2"))),
                            argThat(new ServiceArgMatcher(new ServiceArg(Arrays.asList("-f1", "v1", "-f2", "'v2.1,v2.2,v2.3'", "-f3", "v3.1, v3.2"))))
                    );
                    Mockito.verify(genericAsyncServiceProcessor).process(
                            any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceName", "s3"))),
                            argThat(new ServiceArgMatcher(new ServiceArg(Arrays.asList("-f1", "v1", "-f2", "'v2.1,v2.2,v2.3'", "-f3", "v3.1, v3.2"))))
                    );
                    Mockito.verify(genericAsyncServiceProcessor, times(3)).process(any(JacsServiceData.class));
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

    @Test
    public void processPipelineWithSpecifiedRunningSteps() {
        JacsServiceData testServiceData = createTestService(null, createPipelineConfig(Arrays.asList("s1", "s3")));
        ServiceComputation<JacsServiceResult<Void>> pipelineComputation = pipelineServiceProcessor.process(testServiceData);
        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        pipelineComputation
                .thenApply(r -> {
                    successful.accept(r);
                    Mockito.verify(genericAsyncServiceProcessor).process(
                            any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceName", "s1"))),
                            argThat(new ServiceArgMatcher(new ServiceArg(Arrays.asList("-f1", "v1", "-f2", "'v2.1,v2.2,v2.3'", "-f3", "v3.1, v3.2"))))
                    );
                    Mockito.verify(genericAsyncServiceProcessor, never()).process(
                            any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceName", "s2"))),
                            argThat(new ServiceArgMatcher(new ServiceArg(Arrays.asList("-f1", "v1", "-f2", "'v2.1,v2.2,v2.3'", "-f3", "v3.1, v3.2"))))
                    );
                    Mockito.verify(genericAsyncServiceProcessor).process(
                            any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-serviceName", "s3"))),
                            argThat(new ServiceArgMatcher(new ServiceArg(Arrays.asList("-f1", "v1", "-f2", "'v2.1,v2.2,v2.3'", "-f3", "v3.1, v3.2"))))
                    );
                    Mockito.verify(genericAsyncServiceProcessor, times(2)).process(any(JacsServiceData.class));
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

    private JacsServiceData createTestService(String configURL, Map<String, Object> dictionaryArgs) {
        JacsServiceData testServiceData = new JacsServiceDataBuilder(null)
                .addArgs(StringUtils.isBlank(configURL) ? Arrays.asList() : Arrays.asList("-configURL", configURL))
                .setDictionaryArgs(dictionaryArgs)
                .build();
        testServiceData.setId(testServiceId);
        return testServiceData;
    }

    private Map<String, Object> createPipelineConfig(List<String> runningSteps) {
        ImmutableMap.Builder<String, Object> pipelineConfigBuilder = ImmutableMap.<String, Object>builder();
        if (runningSteps != null) {
            pipelineConfigBuilder.put("runningSteps", runningSteps);
        }
        pipelineConfigBuilder.put("pipelineServices",
                Arrays.asList(
                        createServiceConfig("s1"),
                        createServiceConfig("s2"),
                        createServiceConfig("s3")));
        return ImmutableMap.of("pipelineConfig", pipelineConfigBuilder.build());
    }

    private Map<String, Object> createServiceConfig(String serviceName) {
        return ImmutableMap.<String, Object>builder()
                .put("serviceName", serviceName)
                .put("serviceArgs", Arrays.asList("-f1", "v1", "-f2", "'v2.1,v2.2,v2.3'", "-f3", "v3.1, v3.2"))
                .put("serviceResources", ImmutableMap.of("r1", "rv1", "r2", "rv2", "r3", "rv3"))
                .put("serviceKeyArgs", ImmutableMap.of("k1", "kv1", "k2", "kv2", "k3", "kv3"))
                .build();
    }
}
