package org.janelia.jacs2.asyncservice.lightsheetservices;

import com.google.common.collect.ImmutableMap;
import org.janelia.jacs2.asyncservice.common.ComputationTestUtils;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgMatcher;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.containerizedservices.SingularityContainerProcessor;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.ApplicationConfigProvider;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.utils.HttpUtils;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
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
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        LightsheetPipelineStepProcessor.class, HttpUtils.class
})
public class LightsheetPipelineStepProcessorTest {

    private final static String CONFIG_IP_ARG = "testhost:4000";

    private final static Map<LightsheetPipelineStep, Map<String, Object>> CLUSTER_STEP_CONFIGS = ImmutableMap.of(
            LightsheetPipelineStep.clusterCS, ImmutableMap.<String, Object>builder()
                    .put("dataType", 2)
                    .put("percentile", new int[]{2, 3, 4})
                    .put("outputType", 1)
                    .build(),
            LightsheetPipelineStep.clusterFR, ImmutableMap.<String, Object>builder()
                    .put("filterMode", 1)
                    .put("rangeArray", new int[]{10, 20})
                    .put("timepoints", ImmutableMap.of("start", 10, "every", 5, "end", 100))
                    .build()
            );
    private final static Map<LightsheetPipelineStep, Map<String, Object>> LOCAL_STEP_CONFIGS = ImmutableMap.of(
            LightsheetPipelineStep.localAP, ImmutableMap.<String, Object>builder()
                    .put("dataType", 2)
                    .put("percentile", new int[]{2, 3, 4})
                    .put("outputType", 1)
                    .build(),
            LightsheetPipelineStep.localEC, ImmutableMap.<String, Object>builder()
                    .put("filterMode", 1)
                    .put("rangeArray", new int[]{10, 20})
                    .put("timepoints", ImmutableMap.of("start", 10, "every", 5, "end", 100))
                    .build()
    );

    private File testDirectory;
    private Client testHttpClient;
    private ServiceComputationFactory serviceComputationFactory;
    private LightsheetPipelineStepProcessor lightsheetPipelineStepProcessor;
    private SingularityContainerProcessor containerProcessor;

    @Before
    public void setUp() throws Exception {
        Logger logger = mock(Logger.class);
        serviceComputationFactory = ComputationTestUtils.createTestServiceComputationFactory(logger);
        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        containerProcessor = mock(SingularityContainerProcessor.class);

        ApplicationConfig applicationConfig = new ApplicationConfigProvider().fromMap(
                ImmutableMap.of(
                        "Container.Registry.URL", "shub://imagecatcher",
                        "ImageProcessing.Collection","{Container.Registry.URL}/imageprocessing",
                        "ImageProcessing.Lightsheet.Version", "1.0"
                ))
                .build();

        testDirectory = Files.createTempDirectory("testLightsheetStepProcessor").toFile();
        testHttpClient = Mockito.mock(Client.class);
        PowerMockito.mockStatic(HttpUtils.class);
        Mockito.when(HttpUtils.createHttpClient()).thenReturn(testHttpClient);

        lightsheetPipelineStepProcessor = new LightsheetPipelineStepProcessor(serviceComputationFactory,
                jacsServiceDataPersistence,
                testDirectory.getAbsolutePath(),
                applicationConfig,
                containerProcessor,
                ObjectMapperFactory.instance().getDefaultObjectMapper(),
                logger);
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deletePath(testDirectory.toPath());
    }

    @Test
    public void processLocalStepJob() {
        WebTarget configEndpoint = prepareConfigEnpointTestTarget();
        Mockito.when(testHttpClient.target(CONFIG_IP_ARG)).thenReturn(configEndpoint);
        LOCAL_STEP_CONFIGS.forEach((step, config) -> {
            int stepIndex = 1;
            JacsServiceData testServiceData = createTestService(step, stepIndex, 100, 10, config);
            Mockito.when(containerProcessor.process(any(ServiceExecutionContext.class),
                    any(ServiceArg.class),
                    any(ServiceArg.class),
                    any(ServiceArg.class)))
                    .then(invocation -> serviceComputationFactory.newCompletedComputation(new JacsServiceResult<Void>(testServiceData)))
                    ;

            ServiceComputation<JacsServiceResult<Void>> stepComputation = lightsheetPipelineStepProcessor.process(testServiceData);
            Consumer successful = mock(Consumer.class);
            Consumer failure = mock(Consumer.class);
            stepComputation
                    .thenApply(r -> {
                        successful.accept(r);
                        File stepConfigFile = new File(
                                testDirectory,
                                testServiceData.getId() + "/" + "stepConfig_" + stepIndex + "_" + step + ".json");
                        assertTrue(stepConfigFile.exists());
                        Mockito.verify(containerProcessor).process(
                                any(ServiceExecutionContext.class),
                                argThat(new ServiceArgMatcher(new ServiceArg("-containerLocation", "shub://imagecatcher/imageprocessing/" + step.name().toLowerCase() + ":1.0"))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-bindPaths", stepConfigFile.getParentFile().getAbsolutePath() + ":" + stepConfigFile.getParentFile().getAbsolutePath()))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-appArgs", stepConfigFile.getAbsolutePath())))
                        );
                        Mockito.reset(containerProcessor);
                        return r;
                    })
                    .exceptionally(exc -> {
                        failure.accept(exc);
                        fail(exc.toString());
                        return null;
                    })
            ;
            Mockito.verify(successful).accept(any());
        });
    }

    @Test
    public void processSingleClusterStepJob() {
        WebTarget configEndpoint = prepareConfigEnpointTestTarget();
        Mockito.when(testHttpClient.target(CONFIG_IP_ARG)).thenReturn(configEndpoint);
        CLUSTER_STEP_CONFIGS.forEach((step, config) -> {
            int stepIndex = 1;
            int timePointsPerJob = 10;
            JacsServiceData testServiceData = createTestService(step, stepIndex, 10, timePointsPerJob, config);
            Mockito.when(containerProcessor.process(any(ServiceExecutionContext.class),
                    any(ServiceArg.class),
                    any(ServiceArg.class),
                    any(ServiceArg.class)))
                    .then(invocation -> serviceComputationFactory.newCompletedComputation(new JacsServiceResult<Void>(testServiceData)))
            ;

            ServiceComputation<JacsServiceResult<Void>> stepComputation = lightsheetPipelineStepProcessor.process(testServiceData);
            Consumer successful = mock(Consumer.class);
            Consumer failure = mock(Consumer.class);
            stepComputation
                    .thenApply(r -> {
                        successful.accept(r);
                        File stepConfigFile = new File(
                                testDirectory,
                                testServiceData.getId() + "/" + "stepConfig_" + stepIndex + "_" + step + ".json");
                        assertTrue(stepConfigFile.exists());
                        Mockito.verify(containerProcessor).process(
                                any(ServiceExecutionContext.class),
                                argThat(new ServiceArgMatcher(new ServiceArg("-containerLocation", "shub://imagecatcher/imageprocessing/" + step.name().toLowerCase() + ":1.0"))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-bindPaths", stepConfigFile.getParentFile().getAbsolutePath() + ":" + stepConfigFile.getParentFile().getAbsolutePath()))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-appArgs", stepConfigFile.getAbsolutePath() + "," + String.valueOf(timePointsPerJob) + "," + "1")))
                        );
                        Mockito.reset(containerProcessor);
                        return r;
                    })
                    .exceptionally(exc -> {
                        failure.accept(exc);
                        fail(exc.toString());
                        return null;
                    })
            ;
            Mockito.verify(successful).accept(any());
        });
    }

    @Test
    public void processMultipleClusterStepJob() {
        WebTarget configEndpoint = prepareConfigEnpointTestTarget();
        Mockito.when(testHttpClient.target(CONFIG_IP_ARG)).thenReturn(configEndpoint);
        CLUSTER_STEP_CONFIGS.forEach((step, config) -> {
            int stepIndex = 1;
            int timePoints = 95;
            int timePointsPerJob = 10;
            JacsServiceData testServiceData = createTestService(step, stepIndex, timePoints, timePointsPerJob, config);
            Mockito.when(containerProcessor.process(any(ServiceExecutionContext.class),
                    any(ServiceArg.class),
                    any(ServiceArg.class),
                    any(ServiceArg.class)))
                    .then(invocation -> serviceComputationFactory.newCompletedComputation(new JacsServiceResult<Void>(testServiceData)))
            ;

            ServiceComputation<JacsServiceResult<Void>> stepComputation = lightsheetPipelineStepProcessor.process(testServiceData);
            Consumer successful = mock(Consumer.class);
            Consumer failure = mock(Consumer.class);
            stepComputation
                    .thenApply(r -> {
                        successful.accept(r);
                        File stepConfigFile = new File(
                                testDirectory,
                                testServiceData.getId() + "/" + "stepConfig_" + stepIndex + "_" + step + ".json");
                        assertTrue(stepConfigFile.exists());
                        int numJobs = (int) Math.ceil((double)timePoints / timePointsPerJob);
                        for (int j = 0; j < numJobs; j++) {
                            Mockito.verify(containerProcessor).process(
                                    any(ServiceExecutionContext.class),
                                    argThat(new ServiceArgMatcher(new ServiceArg("-containerLocation", "shub://imagecatcher/imageprocessing/" + step.name().toLowerCase() + ":1.0"))),
                                    argThat(new ServiceArgMatcher(new ServiceArg("-bindPaths", stepConfigFile.getParentFile().getAbsolutePath() + ":" + stepConfigFile.getParentFile().getAbsolutePath()))),
                                    argThat(new ServiceArgMatcher(new ServiceArg("-appArgs", stepConfigFile.getAbsolutePath() + "," + String.valueOf(timePointsPerJob) + "," + String.valueOf(j + 1))))
                            );
                        }
                        Mockito.reset(containerProcessor);
                        return r;
                    })
                    .exceptionally(exc -> {
                        failure.accept(exc);
                        fail(exc.toString());
                        return null;
                    })
            ;
            Mockito.verify(successful).accept(any());
        });
    }

    @Test
    public void processGenerateMinistacks() {
        WebTarget configEndpoint = prepareConfigEnpointTestTarget();
        LOCAL_STEP_CONFIGS.forEach((step, config) -> {
            int stepIndex = 1;
            Mockito.when(testHttpClient.target(CONFIG_IP_ARG + "?stepName=" + step)).thenReturn(configEndpoint);
            JacsServiceData testServiceData = createMinistacksTestService(step, stepIndex, config);
            Mockito.when(containerProcessor.process(any(ServiceExecutionContext.class),
                    any(ServiceArg.class),
                    any(ServiceArg.class),
                    any(ServiceArg.class)))
                    .thenReturn(serviceComputationFactory.newCompletedComputation(new JacsServiceResult<>(testServiceData)))
                    ;
            ServiceComputation<JacsServiceResult<Void>> ministacksComputation = lightsheetPipelineStepProcessor.process(testServiceData);
            Consumer successful = mock(Consumer.class);
            Consumer failure = mock(Consumer.class);
            ministacksComputation
                    .thenApply(r -> {
                        successful.accept(r);
                        File stepConfigFile = new File(
                                testDirectory,
                                testServiceData.getId() + "/" + "stepConfig_" + stepIndex + "_" + step + ".json");
                        assertTrue(stepConfigFile.exists());
                        Mockito.verify(containerProcessor).process(
                                any(ServiceExecutionContext.class),
                                argThat(new ServiceArgMatcher(new ServiceArg("-containerLocation", "shub://imagecatcher/imageprocessing/generateministacks:1.0"))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-bindPaths", stepConfigFile.getParentFile().getAbsolutePath() + ":" + stepConfigFile.getParentFile().getAbsolutePath()))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-appArgs", stepConfigFile.getAbsolutePath())))
                        );
                        Mockito.reset(containerProcessor);
                        return r;
                    })
                    .exceptionally(exc -> {
                        failure.accept(exc);
                        fail(exc.toString());
                        return null;
                    })
            ;
            Mockito.verify(successful).accept(any());
        });
    }

    @Test
    public void processGenerateMinistacksWithBadConfigAddress() {
        WebTarget configEndpoint = prepareConfigEnpointTestTarget();
        LOCAL_STEP_CONFIGS.forEach((step, config) -> {
            int stepIndex = 1;
            Mockito.when(testHttpClient.target(CONFIG_IP_ARG + "?stepName=" + step)).thenReturn(configEndpoint);
            JacsServiceData testServiceData = createMinistacksTestService(null, stepIndex, config);
            Mockito.when(containerProcessor.process(any(ServiceExecutionContext.class),
                    any(ServiceArg.class),
                    any(ServiceArg.class),
                    any(ServiceArg.class)))
                    .thenReturn(serviceComputationFactory.newCompletedComputation(new JacsServiceResult<>(testServiceData)))
            ;
            ServiceComputation<JacsServiceResult<Void>> ministacksComputation = lightsheetPipelineStepProcessor.process(testServiceData);
            Consumer successful = mock(Consumer.class);
            Consumer failure = mock(Consumer.class);
            ministacksComputation
                    .thenApply(r -> {
                        successful.accept(r);
                        return r;
                    })
                    .exceptionally(exc -> {
                        failure.accept(exc);
                        File stepConfigFile = new File(
                                testDirectory,
                                testServiceData.getId() + "/" + "stepConfig_" + stepIndex + "_" + step + ".json");
                        assertFalse(stepConfigFile.exists());
                        Mockito.verify(containerProcessor, never()).process(
                                any(ServiceExecutionContext.class),
                                any(ServiceArg.class),
                                any(ServiceArg.class),
                                any(ServiceArg.class)
                        );
                        Mockito.reset(containerProcessor);
                        return null;
                    })
            ;
            Mockito.verify(successful, never()).accept(any());
            Mockito.verify(failure).accept(any());
        });
    }

    private WebTarget prepareConfigEnpointTestTarget() {
        WebTarget configEndpoint = Mockito.mock(WebTarget.class);
        Invocation.Builder configRequestBuilder = Mockito.mock(Invocation.Builder.class);
        Response configResponse = Mockito.mock(Response.class);
        Mockito.when(configEndpoint.queryParam(anyString(), anyString())).thenReturn(configEndpoint);
        Mockito.when(configEndpoint.request()).thenReturn(configRequestBuilder);
        Mockito.when(configRequestBuilder.get()).thenReturn(configResponse);
        Mockito.when(configResponse.getStatus()).thenReturn(200);
        String testData = "{\"key\": \"val\"}";
        Mockito.when(configResponse.readEntity(InputStream.class)).then(invocation -> new ByteArrayInputStream(testData.getBytes()));
        return configEndpoint;
    }

    private JacsServiceData createTestService(LightsheetPipelineStep step, int stepIndex, int timePoints, int timePointsPerJob, Map<String, Object> dictionaryArgs) {
        JacsServiceData testServiceData = new JacsServiceDataBuilder(null)
                .setWorkspace(testDirectory.getAbsolutePath())
                .addArgs("-step", step.name())
                .addArgs("-stepIndex", String.valueOf(stepIndex))
                .addArgs("-configAddress", CONFIG_IP_ARG)
                .addArgs("-numTimePoints", String.valueOf(timePoints))
                .addArgs("-timePointsPerJob", String.valueOf(timePointsPerJob))
                .setDictionaryArgs(dictionaryArgs)
                .build();
        testServiceData.setId(21L);
        return testServiceData;
    }

    private JacsServiceData createMinistacksTestService(LightsheetPipelineStep step, int stepIndex, Map<String, Object> dictionaryArgs) {
        JacsServiceData testServiceData = new JacsServiceDataBuilder(null)
                .setWorkspace(testDirectory.getAbsolutePath())
                .addArgs("-step", "generateMiniStacks")
                .addArgs("-stepIndex", String.valueOf(stepIndex))
                .addArgs("-configAddress", CONFIG_IP_ARG + (step != null ? "?stepName=" + step.name() : ""))
                .setDictionaryArgs(dictionaryArgs)
                .build();
        testServiceData.setId(21L);
        return testServiceData;
    }

}
