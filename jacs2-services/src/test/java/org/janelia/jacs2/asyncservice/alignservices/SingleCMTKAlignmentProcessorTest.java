package org.janelia.jacs2.asyncservice.alignservices;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.janelia.jacs2.asyncservice.common.ComputationTestUtils;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.ApplicationConfigProvider;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class SingleCMTKAlignmentProcessorTest {
    private static final Long TEST_SERVICE_ID = 1L;
    private static final String TEST_DATADIR = "src/test/resources/testdata/cmtkalign/collectResults";
    private static final String TEST_WORKING_DIR = "testWorkingDir";
    private static final String CMTK_ALIGNER = "cmtkAligner";
    private static final String CMTK_TOOLS_DIR = "cmtkToolsDir";
    private static final String CMTK_DEFAULT_TEMPLATE = "cmtkTemplate";
    private static final String CMTK_LIBRARY_NAME = "cmtkLibraryName";

    private SingleCMTKAlignmentProcessor singleCMTKAlignmentProcessor;
    private Path testDirectory;

    @Before
    public void setUp() throws IOException {
        Logger logger = mock(Logger.class);

        ServiceComputationFactory computationFactory = ComputationTestUtils.createTestServiceComputationFactory(logger);

        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        JacsJobInstanceInfoDao jacsJobInstanceInfoDao = mock(JacsJobInstanceInfoDao.class);

        ApplicationConfig applicationConfig = new ApplicationConfigProvider().fromMap(
                ImmutableMap.of())
                .build();

        singleCMTKAlignmentProcessor = new SingleCMTKAlignmentProcessor(computationFactory,
                jacsServiceDataPersistence,
                null, // serviceRunners are not essential for these unit tests
                TEST_WORKING_DIR,
                CMTK_ALIGNER,
                CMTK_TOOLS_DIR,
                CMTK_DEFAULT_TEMPLATE,
                CMTK_LIBRARY_NAME,
                jacsJobInstanceInfoDao,
                applicationConfig,
                logger);

        testDirectory = Files.createTempDirectory("testcmtkresults");
        Path testDataDir = Paths.get(TEST_DATADIR);
        Files.walkFileTree(testDataDir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                Path relativePath = testDataDir.relativize(dir);
                Path targetPath = testDirectory.resolve(relativePath);
                Files.createDirectories(targetPath);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Path relativePath = testDataDir.relativize(file);
                Path targetFile = testDirectory.resolve(relativePath);
                Files.copy(file, targetFile);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deletePath(testDirectory);
    }

    @Test
    public void collectResults() {
        JacsServiceData testServiceData = createTestServiceData(TEST_SERVICE_ID, ImmutableList.of("i1_01.nrrd", "i1_02.nrrd"));
        ServiceResultHandler<CMTKAlignmentResultFiles> cmtkResultHandler = singleCMTKAlignmentProcessor.getResultHandler();
        CMTKAlignmentResultFiles cmtkResult = cmtkResultHandler.collectResult(new JacsServiceResult<>(testServiceData));
        assertEquals(testDirectory.toString(), cmtkResult.getResultDir());
        List<Path> reformattedFiles = cmtkResult.getReformattedFiles().stream().map(rf -> Paths.get(rf)).collect(Collectors.toList());
        Path affineRegistrationResultsDir = Paths.get(cmtkResult.getAffineRegistrationResultsDir());
        Path warpRegistrationResultsDir = Paths.get(cmtkResult.getWarpRegistrationResultsDir());
        assertTrue(affineRegistrationResultsDir.toFile().exists());
        assertTrue(warpRegistrationResultsDir.toFile().exists());
        reformattedFiles.forEach(rf -> {
            assertTrue(rf.toFile().exists());
            assertThat(testDirectory.relativize(rf), equalTo(Paths.get("reformatted", rf.getFileName().toString())));
        });
        assertThat(testDirectory.relativize(affineRegistrationResultsDir), equalTo(Paths.get("Registration/affine", affineRegistrationResultsDir.getFileName().toString())));
        assertThat(testDirectory.relativize(warpRegistrationResultsDir), equalTo(Paths.get("Registration/warp", warpRegistrationResultsDir.getFileName().toString())));
    }

    private JacsServiceData createTestServiceData(Long serviceId, List<String> inputImages) {
        JacsServiceDataBuilder testServiceDataBuilder = new JacsServiceDataBuilder(null)
                .setOwnerKey("testOwner")
                .addArgs("-inputImages", String.join(",", inputImages))
                .addArgs("-outputDir", testDirectory.toString())
                .setWorkspace(TEST_WORKING_DIR);

        JacsServiceData testServiceData = testServiceDataBuilder.build();
        testServiceData.setId(serviceId);
        return testServiceData;
    }

}
