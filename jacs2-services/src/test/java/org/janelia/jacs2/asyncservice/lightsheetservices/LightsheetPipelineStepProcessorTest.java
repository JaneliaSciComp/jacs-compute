package org.janelia.jacs2.asyncservice.lightsheetservices;

import com.google.common.collect.ImmutableMap;
import com.sun.javafx.scene.shape.PathUtils;
import org.janelia.jacs2.asyncservice.common.ComputationTestUtils;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.ApplicationConfigProvider;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import javax.enterprise.inject.Instance;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class LightsheetPipelineStepProcessorTest {

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
    private LightsheetPipelineStepProcessor lightsheetPipelineStepProcessor;

    @Before
    public void setUp() throws IOException {
        Logger logger = mock(Logger.class);
        ServiceComputationFactory serviceComputationFactory = ComputationTestUtils.createTestServiceComputationFactory(logger);
        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        Instance<ExternalProcessRunner> serviceRunners = mock(Instance.class);
        JacsJobInstanceInfoDao jacsJobInstanceInfoDao = mock(JacsJobInstanceInfoDao.class);

        ApplicationConfig applicationConfig = new ApplicationConfigProvider().fromMap(
                ImmutableMap.of("LightsheetPipeline.Bin.Path", "lightsheetExecutable"))
                .build();

        testDirectory = Files.createTempDirectory("testLightsheetStepProcessor").toFile();

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
        TEST_USER_CONFIGS.forEach((step, config) -> {
            JacsServiceData testServiceData = createTestService(step, 1);
            lightsheetPipelineStepProcessor.prepareConfigurationFiles(testServiceData);
        });
        // TODO
    }

    private JacsServiceData createTestService(LightsheetPipelineStep step, int stepIndex) {
        JacsServiceData testServiceData = new JacsServiceDataBuilder(null)
                .setWorkspace(testDirectory.getAbsolutePath())
                .addArgs("-step", step.name())
                .addArgs("-stepIndex", String.valueOf(stepIndex))
                .setDictionaryArgs(TEST_USER_CONFIGS.get(step))
                .build();
        testServiceData.setId(21L);
        return testServiceData;
    }
}
