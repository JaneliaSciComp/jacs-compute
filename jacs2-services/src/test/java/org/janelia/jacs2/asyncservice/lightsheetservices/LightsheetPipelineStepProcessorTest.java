package org.janelia.jacs2.asyncservice.lightsheetservices;

import com.google.common.collect.ImmutableMap;
import org.janelia.jacs2.asyncservice.common.ComputationTestUtils;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgMatcher;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.containerizedservices.PullSingularityContainerProcessor;
import org.janelia.jacs2.asyncservice.containerizedservices.SimpleRunSingularityContainerProcessor;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.ApplicationConfigProvider;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
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
    private static final Long TEST_SERVICE_ID = 21L;

    private File testDirectory;
    private Client testHttpClient;
    private LightsheetPipelineStepProcessor lightsheetPipelineStepProcessor;
    private PullSingularityContainerProcessor pullContainerProcessor;
    private SimpleRunSingularityContainerProcessor runContainerProcessor;

    @Before
    public void setUp() throws Exception {
        Logger logger = mock(Logger.class);
        ServiceComputationFactory serviceComputationFactory = ComputationTestUtils.createTestServiceComputationFactory(logger);
        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        pullContainerProcessor = mockPullContainerProcessor();
        runContainerProcessor = mockRunContainerProcessor();

        ApplicationConfig applicationConfig = new ApplicationConfigProvider().fromMap(
                ImmutableMap.of(
                        "Container.Registry.URL", "shub://imagecatcher",
                        "ImageProcessing.Collection","{Container.Registry.URL}/imageprocessing",
                        "ImageProcessing.Lightsheet.Version", "1.0",
                        "ImageProcessing.Lightsheet.DataMountPoints", "/groups/lightsheet/lightsheet:/groups/lightsheet/lightsheet,/misc/local,:d1/d1.1,d2:"
                ))
                .build();

        testDirectory = Files.createTempDirectory("testLightsheetStepProcessor").toFile();
        testHttpClient = Mockito.mock(Client.class);
        PowerMockito.mockStatic(HttpUtils.class);
        Mockito.when(HttpUtils.createHttpClient()).thenReturn(testHttpClient);

        Mockito.when(jacsServiceDataPersistence.findById(any(Number.class))).then(invocation -> {
            JacsServiceData sd = new JacsServiceData();
            sd.setId(invocation.getArgument(0));
            sd.setState(JacsServiceState.SUCCESSFUL);
            return sd;
        });
        Mockito.when(jacsServiceDataPersistence.createServiceIfNotFound(any(JacsServiceData.class))).then(invocation -> {
            JacsServiceData jacsServiceData = invocation.getArgument(0);
            jacsServiceData.setId(TEST_SERVICE_ID);
            jacsServiceData.setState(JacsServiceState.SUCCESSFUL); // mark the service as completed otherwise the computation doesn't return
            return jacsServiceData;
        });

        lightsheetPipelineStepProcessor = new LightsheetPipelineStepProcessor(serviceComputationFactory,
                jacsServiceDataPersistence,
                testDirectory.getAbsolutePath(),
                applicationConfig,
                pullContainerProcessor,
                runContainerProcessor,
                ObjectMapperFactory.instance().getDefaultObjectMapper(),
                logger);
    }

    private PullSingularityContainerProcessor mockPullContainerProcessor() {
        PullSingularityContainerProcessor pullContainerProcessor = mock(PullSingularityContainerProcessor.class);
        Mockito.when(pullContainerProcessor.getMetadata()).thenCallRealMethod();
        Mockito.when(pullContainerProcessor.createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class)
        )).thenCallRealMethod();
        return pullContainerProcessor;
    }

    private SimpleRunSingularityContainerProcessor mockRunContainerProcessor() {
        SimpleRunSingularityContainerProcessor runContainerProcessor = mock(SimpleRunSingularityContainerProcessor.class);
        Mockito.when(runContainerProcessor.getMetadata()).thenCallRealMethod();
        Mockito.when(runContainerProcessor.createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class)
        )).thenCallRealMethod();
        return runContainerProcessor;
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deletePath(testDirectory.toPath());
    }

    @Test
    public void processWithNonDefaultContainerLocation() {
        WebTarget configEndpoint = prepareConfigEnpointTestTarget();
        Mockito.when(testHttpClient.target(CONFIG_IP_ARG)).thenReturn(configEndpoint);
        LOCAL_STEP_CONFIGS.forEach((step, config) -> {
            int stepIndex = 1;
            JacsServiceData testServiceData = createTestService(step, stepIndex, 100, 10, config);
            testServiceData.getDictionaryArgs().put("containerImage", "shub://otherregistry/imageprocessing/" + step.name().toLowerCase() + ":latest");
            prepareResultHandlers(step);
            ServiceComputation<JacsServiceResult<Void>> stepComputation = lightsheetPipelineStepProcessor.process(testServiceData);
            @SuppressWarnings("unchecked")
            Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
            @SuppressWarnings("unchecked")
            Consumer<Throwable> failure = mock(Consumer.class);
            stepComputation
                    .thenApply(r -> {
                        successful.accept(r);
                        File stepConfigFile = new File(
                                testDirectory,
                                testServiceData.getId() + "/" + "stepConfig_" + stepIndex + "_" + step + ".json");
                        assertTrue(stepConfigFile.exists());
                        Mockito.verify(runContainerProcessor).createServiceData(
                                any(ServiceExecutionContext.class),
                                argThat(new ServiceArgMatcher(new ServiceArg("-containerLocation", new File(testDirectory, step.name().toLowerCase() + ".simg").getAbsolutePath()))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-bindPaths",
                                        stepConfigFile.getParentFile().getAbsolutePath() + ":" + stepConfigFile.getParentFile().getAbsolutePath() + "," +
                                                "/groups/lightsheet/lightsheet:/groups/lightsheet/lightsheet" + "," +
                                                "/misc/local" + "," +
                                                "d1/d1.1:d1/d1.1" + "," +
                                                "d2"
                                ))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-appArgs", stepConfigFile.getAbsolutePath())))
                        );
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
    public void processLocalStepJob() {
        WebTarget configEndpoint = prepareConfigEnpointTestTarget();
        Mockito.when(testHttpClient.target(CONFIG_IP_ARG)).thenReturn(configEndpoint);
        LOCAL_STEP_CONFIGS.forEach((step, config) -> {
            int stepIndex = 1;
            JacsServiceData testServiceData = createTestService(step, stepIndex, 100, 10, config);
            prepareResultHandlers(step);
            ServiceComputation<JacsServiceResult<Void>> stepComputation = lightsheetPipelineStepProcessor.process(testServiceData);
            @SuppressWarnings("unchecked")
            Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
            @SuppressWarnings("unchecked")
            Consumer<Throwable> failure = mock(Consumer.class);
            stepComputation
                    .thenApply(r -> {
                        successful.accept(r);
                        File stepConfigFile = new File(
                                testDirectory,
                                testServiceData.getId() + "/" + "stepConfig_" + stepIndex + "_" + step + ".json");
                        assertTrue(stepConfigFile.exists());
                        Mockito.verify(runContainerProcessor).createServiceData(
                                any(ServiceExecutionContext.class),
                                argThat(new ServiceArgMatcher(new ServiceArg("-containerLocation", new File(testDirectory, step.name().toLowerCase() + ".simg").getAbsolutePath()))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-bindPaths",
                                        stepConfigFile.getParentFile().getAbsolutePath() + ":" + stepConfigFile.getParentFile().getAbsolutePath() + "," +
                                                "/groups/lightsheet/lightsheet:/groups/lightsheet/lightsheet" + "," +
                                                "/misc/local" + "," +
                                                "d1/d1.1:d1/d1.1" + "," +
                                                "d2"
                                ))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-appArgs", stepConfigFile.getAbsolutePath())))
                        );
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
            prepareResultHandlers(step);
            ServiceComputation<JacsServiceResult<Void>> stepComputation = lightsheetPipelineStepProcessor.process(testServiceData);
            @SuppressWarnings("unchecked")
            Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
            @SuppressWarnings("unchecked")
            Consumer<Throwable> failure = mock(Consumer.class);
            stepComputation
                    .thenApply(r -> {
                        successful.accept(r);
                        File stepConfigFile = new File(
                                testDirectory,
                                testServiceData.getId() + "/" + "stepConfig_" + stepIndex + "_" + step + ".json");
                        assertTrue(stepConfigFile.exists());
                        Mockito.verify(runContainerProcessor).createServiceData(
                                any(ServiceExecutionContext.class),
                                argThat(new ServiceArgMatcher(new ServiceArg("-containerLocation", new File(testDirectory, step.name().toLowerCase() + ".simg").getAbsolutePath()))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-bindPaths",
                                        stepConfigFile.getParentFile().getAbsolutePath() + ":" + stepConfigFile.getParentFile().getAbsolutePath() + "," +
                                                "/groups/lightsheet/lightsheet:/groups/lightsheet/lightsheet" + "," +
                                                "/misc/local" + "," +
                                                "d1/d1.1:d1/d1.1" + "," +
                                                "d2"
                                ))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-appArgs", stepConfigFile.getAbsolutePath() + "," + String.valueOf(timePointsPerJob) + "," + "1")))
                        );
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
            prepareResultHandlers(step);
            ServiceComputation<JacsServiceResult<Void>> stepComputation = lightsheetPipelineStepProcessor.process(testServiceData);
            @SuppressWarnings("unchecked")
            Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
            @SuppressWarnings("unchecked")
            Consumer<Throwable> failure = mock(Consumer.class);
            stepComputation
                    .thenApply(r -> {
                        successful.accept(r);
                        File stepConfigFile = new File(
                                testDirectory,
                                testServiceData.getId() + "/" + "stepConfig_" + stepIndex + "_" + step + ".json");
                        assertTrue(stepConfigFile.exists());
                        int numJobs = (int) Math.ceil((double)timePoints / timePointsPerJob);
                        for (int j = 0; j < numJobs; j++) {
                            Mockito.verify(runContainerProcessor).createServiceData(
                                    any(ServiceExecutionContext.class),
                                    argThat(new ServiceArgMatcher(new ServiceArg("-containerLocation", new File(testDirectory, step.name().toLowerCase() + ".simg").getAbsolutePath()))),
                                    argThat(new ServiceArgMatcher(new ServiceArg("-bindPaths",
                                            stepConfigFile.getParentFile().getAbsolutePath() + ":" + stepConfigFile.getParentFile().getAbsolutePath() + "," +
                                                    "/groups/lightsheet/lightsheet:/groups/lightsheet/lightsheet" + "," +
                                                    "/misc/local" + "," +
                                                    "d1/d1.1:d1/d1.1" + "," +
                                                    "d2"
                                    ))),
                                    argThat(new ServiceArgMatcher(new ServiceArg("-appArgs", stepConfigFile.getAbsolutePath() + "," + String.valueOf(timePointsPerJob) + "," + String.valueOf(j + 1))))
                            );
                        }
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
            testServiceData.getDictionaryArgs().putAll(ImmutableMap.<String, Object>builder()
                    .put("inputFolder", "/in")
                    .put("inputString", "/in")
                    .put("outputString", "/tmp/sub")
                    .put("configRoot", "//tmp/config")
                    .put("sourceString", "/tmp/sub")
                    .put("lookUpTable", "/tmp/ltFile")
                    .put("inputRoot", "/inRoot/in")
                    .put("outputRoot", "/tmp/out")
                    .put("inputDir", "/inDir/in")
                    .put("outputDir", "/var/tmp/out")
                    .build()
            );
            prepareResultHandlers(step);
            ServiceComputation<JacsServiceResult<Void>> ministacksComputation = lightsheetPipelineStepProcessor.process(testServiceData);
            @SuppressWarnings("unchecked")
            Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
            @SuppressWarnings("unchecked")
            Consumer<Throwable> failure = mock(Consumer.class);
            ministacksComputation
                    .thenApply(r -> {
                        successful.accept(r);
                        File stepConfigFile = new File(
                                testDirectory,
                                testServiceData.getId() + "/" + "stepConfig_" + stepIndex + "_" + step + ".json");
                        assertTrue(stepConfigFile.exists());
                        Mockito.verify(runContainerProcessor).createServiceData(
                                any(ServiceExecutionContext.class),
                                argThat(new ServiceArgMatcher(new ServiceArg("-containerLocation", new File(testDirectory, step.name().toLowerCase() + ".simg").getAbsolutePath()))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-bindPaths",
                                        stepConfigFile.getParentFile().getAbsolutePath() + ":" + stepConfigFile.getParentFile().getAbsolutePath() + "," +
                                                "/groups/lightsheet/lightsheet:/groups/lightsheet/lightsheet" + "," +
                                                "/misc/local" + "," +
                                                "d1/d1.1:d1/d1.1" + "," +
                                                "d2" + "," +
                                                "/in:/in" + "," +
                                                "/tmp:/tmp" + "," +
                                                "/inRoot/in:/inRoot/in" + "," +
                                                "/inDir/in:/inDir/in" + "," +
                                                "/var/tmp:/var/tmp"
                                ))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-appArgs", stepConfigFile.getAbsolutePath())))
                        );
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
            prepareResultHandlers(step);
            ServiceComputation<JacsServiceResult<Void>> ministacksComputation = lightsheetPipelineStepProcessor.process(testServiceData);
            @SuppressWarnings("unchecked")
            Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
            @SuppressWarnings("unchecked")
            Consumer<Throwable> failure = mock(Consumer.class);
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
                        Mockito.verify(runContainerProcessor, never()).process(
                                any(ServiceExecutionContext.class),
                                any(ServiceArg.class),
                                any(ServiceArg.class),
                                any(ServiceArg.class)
                        );
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

    private void prepareResultHandlers(LightsheetPipelineStep step) {
        @SuppressWarnings("unchecked")
        ServiceResultHandler<File> pullContainerResultHandler = mock(ServiceResultHandler.class);
        Mockito.when(pullContainerResultHandler.getServiceDataResult(any(JacsServiceData.class)))
                .then(invocation -> new File(testDirectory, step.name().toLowerCase() + ".simg"));
        Mockito.when(pullContainerProcessor.getResultHandler()).thenReturn(pullContainerResultHandler);
        Mockito.when(runContainerProcessor.getResultHandler()).thenCallRealMethod();
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
        testServiceData.setId(TEST_SERVICE_ID);
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
        testServiceData.setId(TEST_SERVICE_ID);
        return testServiceData;
    }

}
