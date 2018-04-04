package org.janelia.jacs2.asyncservice.lightsheetservices;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.sun.javafx.scene.shape.PathUtils;
import org.janelia.jacs2.asyncservice.common.ComputationTestUtils;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.ApplicationConfigProvider;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.utils.HttpUtils;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
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

import javax.enterprise.inject.Instance;
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

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        LightsheetPipelineStepProcessor.class, HttpUtils.class
})
public class LightsheetPipelineStepProcessorTest {

    private final static String CONFIG_IP_ARG = "testhost:4000";

    private final static Map<LightsheetPipelineStep, Map<String, Object>> TEST_USER_CONFIGS = ImmutableMap.of(
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
    private File testDirectory;
    private Client testHttpClient;
    private LightsheetPipelineStepProcessor lightsheetPipelineStepProcessor;

    @Before
    public void setUp() throws Exception {
        Logger logger = mock(Logger.class);
        ServiceComputationFactory serviceComputationFactory = ComputationTestUtils.createTestServiceComputationFactory(logger);
        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        Instance<ExternalProcessRunner> serviceRunners = mock(Instance.class);
        JacsJobInstanceInfoDao jacsJobInstanceInfoDao = mock(JacsJobInstanceInfoDao.class);

        ApplicationConfig applicationConfig = new ApplicationConfigProvider().fromMap(
                ImmutableMap.of("LightsheetPipeline.Bin.Path", "lightsheetExecutable"))
                .build();

        testDirectory = Files.createTempDirectory("testLightsheetStepProcessor").toFile();
        testHttpClient = Mockito.mock(Client.class);
        PowerMockito.mockStatic(HttpUtils.class);
        Mockito.when(HttpUtils.createHttpClient()).thenReturn(testHttpClient);

        lightsheetPipelineStepProcessor = new LightsheetPipelineStepProcessor(serviceComputationFactory,
                jacsServiceDataPersistence,
                serviceRunners,
                testDirectory.getAbsolutePath(),
                "lightsheetExecutable",
                jacsJobInstanceInfoDao,
                applicationConfig,
                ObjectMapperFactory.instance().getDefaultObjectMapper(),
                logger);
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deletePath(testDirectory.toPath());
    }

    @Test
    public void prepareConfigFiles() {
        WebTarget configEndpoint = Mockito.mock(WebTarget.class);
        Invocation.Builder configRequestBuilder = Mockito.mock(Invocation.Builder.class);
        Response configResponse = Mockito.mock(Response.class);
        Mockito.when(configEndpoint.queryParam(anyString(), anyString())).thenReturn(configEndpoint);
        Mockito.when(configEndpoint.request()).thenReturn(configRequestBuilder);
        Mockito.when(configRequestBuilder.get()).thenReturn(configResponse);
        Mockito.when(configResponse.getStatus()).thenReturn(200);
        String testData = "{\"key\": \"val\"}";
        Mockito.when(configResponse.readEntity(InputStream.class)).then(invocation -> {
            return new ByteArrayInputStream(testData.getBytes());
        });
        Mockito.when(testHttpClient.target(CONFIG_IP_ARG)).thenReturn(configEndpoint);
        TEST_USER_CONFIGS.forEach((step, config) -> {
            int stepIndex = 1;
            JacsServiceData testServiceData = createTestService(step, stepIndex);
            lightsheetPipelineStepProcessor.prepareConfigurationFiles(testServiceData);
            assertTrue(new File(testDirectory,
                    testServiceData.getId() + "/" + "stepConfig_" + stepIndex + "_" + step + ".json").exists());
        });
    }

    private JacsServiceData createTestService(LightsheetPipelineStep step, int stepIndex) {
        JacsServiceData testServiceData = new JacsServiceDataBuilder(null)
                .setWorkspace(testDirectory.getAbsolutePath())
                .addArgs("-step", step.name())
                .addArgs("-stepIndex", String.valueOf(stepIndex))
                .addArgs("-configAddress", CONFIG_IP_ARG)
                .setDictionaryArgs(TEST_USER_CONFIGS.get(step))
                .build();
        testServiceData.setId(21L);
        return testServiceData;
    }
}
