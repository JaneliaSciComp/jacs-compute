package org.janelia.jacs2.asyncservice.lightsheetservices;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.ComputationTestHelper;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgMatcher;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceProcessorTestHelper;
import org.janelia.jacs2.asyncservice.pipeline.PipelineServiceProcessor;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.testhelpers.ListArgMatcher;
import org.janelia.jacs2.utils.HttpUtils;
import org.janelia.model.jacs2.domain.IndexedReference;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.janelia.model.service.JacsServiceState;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;

import javax.ws.rs.client.Client;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        PipelineServiceProcessor.class, HttpUtils.class
})
public class LightsheetPipelineProcessorTest {

    private static final String TEST_WORKING_DIR = "testDir";
    private static volatile long testServiceId = 21L;

    private Map<Number, JacsServiceData> serviceStorage = new HashMap<>();
    private LightsheetPipelineStepProcessor lightsheetPipelineStepProcessor;
    private LightsheetPipelineProcessor lightsheetPipelineProcessor;
    private Client testHttpClient;

    @Before
    public void setUp() {
        Logger logger = mock(Logger.class);
        ServiceComputationFactory serviceComputationFactory = ComputationTestHelper.createTestServiceComputationFactory(logger);
        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        lightsheetPipelineStepProcessor = mock(LightsheetPipelineStepProcessor.class);
        ServiceProcessorTestHelper.prepareServiceProcessorMetadataAsRealCall(lightsheetPipelineStepProcessor);
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

        testHttpClient = Mockito.mock(Client.class);
        PowerMockito.mockStatic(HttpUtils.class);
        Mockito.when(HttpUtils.createHttpClient()).thenReturn(testHttpClient);

        lightsheetPipelineProcessor = new LightsheetPipelineProcessor(serviceComputationFactory,
                jacsServiceDataPersistence,
                TEST_WORKING_DIR,
                lightsheetPipelineStepProcessor,
                logger);
    }

    @Test
    public void processLightsheetPipeline() {
        String pipelineConfigReference = "pipelineTestConfigID";
        List<String> testSteps = Arrays.asList("clusterCS", "clusterFM", "localAC");
        JacsServiceData testServiceData = createTestService(
                null,
                pipelineConfigReference,
                createPipelineConfig(testSteps));

        Mockito.when(lightsheetPipelineStepProcessor.getResultHandler()).thenCallRealMethod();

        ServiceComputation<JacsServiceResult<Void>> pipelineComputation = lightsheetPipelineProcessor.process(testServiceData);
        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        pipelineComputation
                .thenApply(r -> {
                    successful.accept(r);
                    IndexedReference.indexListContent(
                            testSteps,
                            (stepIndex, stepName) -> new IndexedReference<>(stepName, stepIndex))
                            .map(indexedStep -> new ListArgMatcher<>(ImmutableList.of(
                                    new ServiceArgMatcher(new ServiceArg("-step", indexedStep.getReference())),
                                    new ServiceArgMatcher(new ServiceArg("-stepIndex", indexedStep.getPos())),
                                    new ServiceArgMatcher(new ServiceArg("-configReference", pipelineConfigReference))
                            )))
                            .forEach(argMatcher -> Mockito.verify(lightsheetPipelineStepProcessor).createServiceData(
                                    any(ServiceExecutionContext.class),
                                    argThat(argMatcher)
                            ));
                    Mockito.verify(lightsheetPipelineStepProcessor, times(3)).getMetadata();
                    Mockito.verify(lightsheetPipelineStepProcessor, times(3)).getResultHandler();
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

    private JacsServiceData createTestService(String configURL, String configRefence, Map<String, Object> dictionaryArgs) {
        JacsServiceData testServiceData = new JacsServiceDataBuilder(null)
                .addArgs(StringUtils.isBlank(configURL) ? Arrays.asList() : Arrays.asList("-configURL", configURL))
                .addArgs(StringUtils.isBlank(configRefence) ? Arrays.asList() : Arrays.asList("-configReference", configRefence))
                .setDictionaryArgs(dictionaryArgs)
                .build();
        testServiceData.setId(testServiceId);
        return testServiceData;
    }

    private Map<String, Object> createPipelineConfig(List<String> steps) {
        ImmutableMap.Builder<String, Object> pipelineConfigBuilder = ImmutableMap.<String, Object>builder();
        pipelineConfigBuilder.put("steps",
                steps.stream().map(stepName -> createServiceConfig(stepName)).collect(Collectors.toList()));
        return ImmutableMap.of("pipelineConfig", pipelineConfigBuilder.build());
    }

    private Map<String, Object> createServiceConfig(String stepName) {
        return ImmutableMap.<String, Object>builder()
                .put("name", stepName)
                .put("serviceArgs", Arrays.asList("-f1", "v1", "-f2", "'v2.1,v2.2,v2.3'", "-f3", "v3.1, v3.2"))
                .put("serviceResources", ImmutableMap.of("r1", "rv1", "r2", "rv2", "r3", "rv3"))
                .put("serviceKeyArgs", ImmutableMap.of("k1", "kv1", "k2", "kv2", "k3", "kv3"))
                .build();
    }
}
