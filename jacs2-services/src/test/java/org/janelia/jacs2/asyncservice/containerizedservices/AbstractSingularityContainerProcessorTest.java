package org.janelia.jacs2.asyncservice.containerizedservices;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.janelia.jacs2.asyncservice.common.ComputationTestUtils;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.cdi.ApplicationConfigProvider;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import javax.enterprise.inject.Instance;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

public class AbstractSingularityContainerProcessorTest {

    private final static String TEST_WORKING_DIR = "testDir";
    private final static String TEST_CONTAINER_IMAGES_DIR = TEST_WORKING_DIR + "/containerImages";

    private PullSingularityContainerProcessor testContainerProcessor;

    @Before
    public void setUp() {
        Logger logger = mock(Logger.class);
        ServiceComputationFactory serviceComputationFactory = ComputationTestUtils.createTestServiceComputationFactory(logger);
        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        Instance<ExternalProcessRunner> serviceRunners = mock(Instance.class);
        JacsJobInstanceInfoDao jacsJobInstanceInfoDao = mock(JacsJobInstanceInfoDao.class);

        ApplicationConfig applicationConfig = new ApplicationConfigProvider().fromMap(ImmutableMap.of()).build();

        testContainerProcessor = new PullSingularityContainerProcessor(serviceComputationFactory,
                jacsServiceDataPersistence,
                serviceRunners,
                TEST_WORKING_DIR,
                "singularity",
                TEST_CONTAINER_IMAGES_DIR,
                jacsJobInstanceInfoDao,
                applicationConfig,
                logger);

    }

    @Test
    public void localContainerImage() {
        class TestData {
            private final List<String> inputArgs;
            private final Path expectedResult;

            private TestData(List<String> inputArgs, Path expectedResult) {
                this.inputArgs = inputArgs;
                this.expectedResult = expectedResult;
            }
        }
        TestData[] testData = new TestData[] {
                new TestData(
                        ImmutableList.of("-containerLocation", "shub://server/collection/c1:1.0"),
                        Paths.get(TEST_CONTAINER_IMAGES_DIR, "collection-c1-1.0.simg")
                ),
                new TestData(
                        ImmutableList.of("-containerLocation", "shub://server/collection/c1"),
                        Paths.get(TEST_CONTAINER_IMAGES_DIR, "collection-c1.simg")
                ),
                new TestData(
                        ImmutableList.of("-containerLocation", "shub://collection/c1:1.0"),
                        Paths.get(TEST_CONTAINER_IMAGES_DIR, "collection-c1-1.0.simg")
                ),
                new TestData(
                        ImmutableList.of(
                                "-containerLocation", "shub://collection/c1:1.0",
                                "-containerName", "cname"
                        ),
                        Paths.get(TEST_CONTAINER_IMAGES_DIR, "cname")
                ),
                new TestData(
                        ImmutableList.of(
                                "-containerLocation", "shub://collection/c1:1.0",
                                "-containerName", "cpath/cname"
                        ),
                        Paths.get(TEST_CONTAINER_IMAGES_DIR, "cname")
                ),
                new TestData(
                        ImmutableList.of(
                                "-containerLocation", "docker://image:1.0"
                        ),
                        Paths.get(TEST_CONTAINER_IMAGES_DIR, "image-1.0.simg")
                ),
                new TestData(
                        ImmutableList.of(
                                "-containerLocation", "docker://collection/image:1.0",
                                "-containerImagesDir", TEST_WORKING_DIR
                        ),
                        Paths.get(TEST_WORKING_DIR, "collection-image-1.0.simg")
                ),
                new TestData(
                        ImmutableList.of(
                                "-containerLocation", "/my/local/path/image.simg",
                                "-containerImagesDir", TEST_WORKING_DIR,
                                "-containerName", "othername"
                        ),
                        Paths.get("/my/local/path/image.simg")
                )
        };
        for (TestData td : testData) {
            Path containerImage = testContainerProcessor.getLocalContainerImage(testContainerProcessor.getArgs(createTestService(td.inputArgs)));
            assertEquals("Expected result for " + td.inputArgs + " - " + td.expectedResult, td.expectedResult, containerImage);
        }
    }

    private JacsServiceData createTestService(List<String> args) {
        JacsServiceData testServiceData = new JacsServiceDataBuilder(null)
                .addArgs(args)
                .build();
        return testServiceData;
    }
}
