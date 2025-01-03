package org.janelia.jacs2.asyncservice.dataimport;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.hamcrest.MatcherAssert;
import org.janelia.jacs2.asyncservice.common.ComputationTestHelper;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.cdi.ApplicationConfigProvider;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

public class SageLoaderProcessorTest {
    private static final String PERL_EXECUTABLE = "perl";
    private static final String LIBRARY_PATH = "testLibrary";
    private static final String SCRIPT = "testScript";
    private static final String DEFAULT_WORKING_DIR = "testWorking";
    private static final String EXECUTABLES_BASE_DIR = "testTools";

    private JacsServiceDataPersistence jacsServiceDataPersistence;
    private JacsJobInstanceInfoDao jacsJobInstanceInfoDao;
    private ServiceComputationFactory computationFactory;
    private Logger logger;

    @BeforeEach
    public void setUp() {
        jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        jacsJobInstanceInfoDao = mock(JacsJobInstanceInfoDao.class);
        logger = mock(Logger.class);
        computationFactory = ComputationTestHelper.createTestServiceComputationFactory(logger);
    }

    private SageLoaderProcessor createSageLoaderProcessor(String sageEnvironment) {
        ApplicationConfig applicationConfig = new ApplicationConfigProvider()
                .fromMap(
                        ImmutableMap.<String, String>builder()
                                .put("Executables.ModuleBase", EXECUTABLES_BASE_DIR)
                                .put("Sage.write.environment", sageEnvironment)
                                .build()
                )
                .build();
        return new SageLoaderProcessor(computationFactory,
                jacsServiceDataPersistence,
                null, // serviceRunners are not essential for these unit tests
                DEFAULT_WORKING_DIR,
                PERL_EXECUTABLE,
                LIBRARY_PATH,
                SCRIPT,
                jacsJobInstanceInfoDao,
                applicationConfig,
                logger);
    }

    @Test
    public void notAllImagesFoundInProductionMode() {
        List<String> testImages = ImmutableList.of("im1", "im2", "im3", "im4");
        Long serviceId = 1L;
        String serviceOutPath = "src/test/resources/testdata/sageLoader/notAllImagesFound/serviceOutput";
        String serviceErrPath = "src/test/resources/testdata/sageLoader/notAllImagesFound/serviceErrors";
        JacsServiceData testService = createTestServiceData(serviceId, testImages, serviceOutPath, serviceErrPath);

        SageLoaderProcessor sageLoaderProcessor = createSageLoaderProcessor("production");

        List<String> errors = sageLoaderProcessor.getErrorChecker().collectErrors(testService);
        MatcherAssert.assertThat(errors, hasItem("Not all images found - expected 4 but only found 3 (check that grammar pipeline tools are in the right location)"));
    }

    @Test
    public void allImagesFoundInProductionMode() {
        List<String> testImages = ImmutableList.of("im1", "im2", "im3", "im4");
        Long serviceId = 1L;
        String serviceOutPath = "src/test/resources/testdata/sageLoader/allImagesFound/serviceOutput";
        String serviceErrPath = "src/test/resources/testdata/sageLoader/allImagesFound/serviceErrors";

        JacsServiceData testService = createTestServiceData(serviceId, testImages, serviceOutPath, serviceErrPath);

        SageLoaderProcessor sageLoaderProcessor = createSageLoaderProcessor("production");

        List<String> errors = sageLoaderProcessor.getErrorChecker().collectErrors(testService);
        MatcherAssert.assertThat(errors, hasSize(0));
    }

    @Test
    public void notAllImagesFoundInDevMode() {
        List<String> testImages = ImmutableList.of("im1", "im2", "im3", "im4", "im5");
        Long serviceId = 1L;
        String serviceOutPath = "src/test/resources/testdata/sageLoader/notAllImagesFound/serviceOutput";
        String serviceErrPath = "src/test/resources/testdata/sageLoader/notAllImagesFound/serviceErrors";
        JacsServiceData testService = createTestServiceData(serviceId, testImages, serviceOutPath, serviceErrPath);

        SageLoaderProcessor sageLoaderProcessor = createSageLoaderProcessor("dev");

        List<String> errors = sageLoaderProcessor.getErrorChecker().collectErrors(testService);
        MatcherAssert.assertThat(errors, hasItem("Not all images found - expected 5 but found 3 and inserted 1"));
    }

    @Test
    public void allImagesFoundInDevMode() {
        List<String> testImages = ImmutableList.of("im1", "im2", "im3", "im4");
        Long serviceId = 1L;
        String serviceOutPath = "src/test/resources/testdata/sageLoader/allImagesFound/serviceOutput";
        String serviceErrPath = "src/test/resources/testdata/sageLoader/allImagesFound/serviceErrors";
        JacsServiceData testService = createTestServiceData(serviceId, testImages, serviceOutPath, serviceErrPath);

        SageLoaderProcessor sageLoaderProcessor = createSageLoaderProcessor("dev");

        List<String> errors = sageLoaderProcessor.getErrorChecker().collectErrors(testService);
        MatcherAssert.assertThat(errors, hasSize(0));
    }

    @Test
    public void imagesFoundPlusImagesInsertedMatchInDevMode() {
        List<String> testImages = ImmutableList.of("im1", "im2", "im3", "im4", "im5");
        Long serviceId = 1L;
        String serviceOutPath = "src/test/resources/testdata/sageLoader/specialCaseDevModeMatch/serviceOutput";
        String serviceErrPath = "src/test/resources/testdata/sageLoader/specialCaseDevModeMatch/serviceErrors";
        JacsServiceData testService = createTestServiceData(serviceId, testImages, serviceOutPath, serviceErrPath);

        SageLoaderProcessor sageLoaderProcessor = createSageLoaderProcessor("dev");

        List<String> errors = sageLoaderProcessor.getErrorChecker().collectErrors(testService);
        MatcherAssert.assertThat(errors, hasSize(0));
    }

    @Test
    public void imagesFoundPlusImagesInsertedDoNotMatchInDevMode() {
        List<String> testImages = ImmutableList.of("im1", "im2", "im3", "im4");
        Long serviceId = 1L;
        String serviceOutPath = "src/test/resources/testdata/sageLoader/specialCaseDevModeMatch/serviceOutput";
        String serviceErrPath = "src/test/resources/testdata/sageLoader/specialCaseDevModeMatch/serviceErrors";
        JacsServiceData testService = createTestServiceData(serviceId, testImages, serviceOutPath, serviceErrPath);

        SageLoaderProcessor sageLoaderProcessor = createSageLoaderProcessor("dev");

        List<String> errors = sageLoaderProcessor.getErrorChecker().collectErrors(testService);
        MatcherAssert.assertThat(errors, hasItem("Not all images found - expected 4 but found 3 and inserted 2"));
    }

    @Test
    public void processOutputDirsMissing() {
        List<String> testImages = ImmutableList.of("im1", "im2", "im3", "im4", "im5");
        Long serviceId = 1L;
        String serviceOutPath = "testOutDir";
        String serviceErrPath = "testErrDir";

        JacsServiceData testService = createTestServiceData(serviceId, testImages, serviceOutPath, serviceErrPath);

        try (MockedStatic<Files> mockedFiles = Mockito.mockStatic(Files.class)) {
            mockedFiles.when(() -> Files.isRegularFile(ArgumentMatchers.any(Path.class))).thenReturn(true);
            mockedFiles.when(() -> Files.notExists(Paths.get(serviceOutPath))).thenReturn(true);
            mockedFiles.when(() -> Files.notExists(Paths.get(serviceErrPath))).thenReturn(true);
            SageLoaderProcessor sageLoaderProcessor = createSageLoaderProcessor("dev");

            List<String> errors = sageLoaderProcessor.getErrorChecker().collectErrors(testService);
            MatcherAssert.assertThat(errors, hasItems(
                    "Processor output path not found: " + testService,
                    "Processor error path not found: " + testService));
        }
    }

    private JacsServiceData createTestServiceData(Number serviceId, List<String> sampleFiles, String serviceOutputPath, String serviceErrorPath) {
        JacsServiceDataBuilder testServiceDataBuilder = new JacsServiceDataBuilder(null)
                .setOwnerKey("testOwner")
                .addArgs("-configFile", "config.txt")
                .addArgs("-grammarFile", "grammar.txt")
                .addArgs("-sampleFiles", String.join(",", sampleFiles))
                .setOutputPath(serviceOutputPath)
                .setErrorPath(serviceErrorPath);

        JacsServiceData testServiceData = testServiceDataBuilder.build();
        testServiceData.setId(serviceId);
        return testServiceData;
    }

}
