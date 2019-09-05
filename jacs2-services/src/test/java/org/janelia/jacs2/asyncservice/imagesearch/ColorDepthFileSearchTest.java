package org.janelia.jacs2.asyncservice.imagesearch;

import com.google.common.collect.ImmutableList;
import org.janelia.jacs2.asyncservice.common.ComputationTestHelper;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.cluster.ComputeAccounting;
import org.janelia.jacs2.asyncservice.spark.LSFSparkClusterLauncher;
import org.janelia.jacs2.asyncservice.spark.SparkApp;
import org.janelia.jacs2.asyncservice.spark.SparkCluster;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        ColorDepthFileSearch.class,
        FileUtils.class
})
public class ColorDepthFileSearchTest {

    private static final String TEST_WORKSPACE = "testColorDepthLocalWorkspace";
    private static final String DEFAULT_WORKING_DIR = "testWorking";
    private static final int SEARCH_TIMEOUT_IN_SECONDS = 1200;
    private static final int SEARCH_INTERVAL_CHECK_IN_MILLIS = 5000;
    private static final int DEFAULT_NUM_NODES = 6;
    private static final int DEFAULT_MIN_REQUIRED_WORKERS = 2;

    ServiceComputationFactory serviceComputationFactory;
    private JacsServiceDataPersistence jacsServiceDataPersistence;
    private LSFSparkClusterLauncher clusterLauncher;
    private ComputeAccounting clusterAccounting;
    private SparkCluster sparkCluster;
    private SparkApp sparkApp;
    private String jarPath = "sparkColorDepthSearch.jar";

    private ColorDepthFileSearch colorDepthFileSearch;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        Logger logger = mock(Logger.class);
        serviceComputationFactory = ComputationTestHelper.createTestServiceComputationFactory(logger);
        jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        clusterLauncher = mock(LSFSparkClusterLauncher.class);
        clusterAccounting = mock(ComputeAccounting.class);
        sparkCluster = mock(SparkCluster.class);
        sparkApp = mock(SparkApp.class);
        Mockito.when(sparkApp.isDone()).thenReturn(true);

        colorDepthFileSearch = new ColorDepthFileSearch(serviceComputationFactory,
                jacsServiceDataPersistence,
                DEFAULT_WORKING_DIR,
                clusterLauncher,
                clusterAccounting,
                SEARCH_TIMEOUT_IN_SECONDS,
                SEARCH_INTERVAL_CHECK_IN_MILLIS,
                DEFAULT_NUM_NODES,
                DEFAULT_MIN_REQUIRED_WORKERS,
                jarPath,
                logger);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void process() throws Exception {
        JacsServiceData testService = createTestServiceData(1L, "test");
        JacsServiceFolder serviceWorkingFolder = new JacsServiceFolder(null, Paths.get(testService.getWorkspace()), testService);
        Path serviceOutputPath = serviceWorkingFolder.getServiceFolder(JacsServiceFolder.SERVICE_OUTPUT_DIR);
        Path serviceErrorPath = serviceWorkingFolder.getServiceFolder(JacsServiceFolder.SERVICE_ERROR_DIR);
        String clusterBillingInfo = "clusterBillingInfo";

        PowerMockito.mockStatic(Files.class);
        Mockito.when(Files.createDirectories(any(Path.class))).then((Answer<Path>) invocation -> invocation.getArgument(0));
        Mockito.when(Files.find(any(Path.class), anyInt(), any(BiPredicate.class))).then(invocation -> {
            Path root = invocation.getArgument(0);
            return Stream.of(root.resolve("f1_results.txt"));
        });
        Mockito.when(clusterAccounting.getComputeAccount(testService)).thenReturn(clusterBillingInfo);
        Mockito.when(clusterLauncher.startCluster(
                9,
                3,
                serviceWorkingFolder.getServiceFolder(),
                serviceOutputPath,
                serviceErrorPath,
                clusterBillingInfo,
                null,
                null,
                null,
                (int) (Duration.ofSeconds(SEARCH_TIMEOUT_IN_SECONDS).toMinutes()+ 1)))
                .thenReturn(serviceComputationFactory.newCompletedComputation(sparkCluster));

        Mockito.when(sparkCluster.runApp(
                jarPath,
                null,
                0,
                serviceWorkingFolder.getServiceFolder(JacsServiceFolder.SERVICE_OUTPUT_DIR).toString(),
                serviceWorkingFolder.getServiceFolder(JacsServiceFolder.SERVICE_ERROR_DIR).toString(),
                null,
                (long) SEARCH_INTERVAL_CHECK_IN_MILLIS,
                SEARCH_TIMEOUT_IN_SECONDS * 1000L,
                ImmutableList.of(
                        "-m", "f1", "f2", "f3",
                        "-i", "s1,s2",
                        "--maskThresholds", "100", "100", "100",
                        "--dataThreshold", "100",
                        "--pixColorFluctuation", "2.0",
                        "--xyShift", "20",
                        "--pctPositivePixels", "10.0",
                        "-o",
                        serviceWorkingFolder.getServiceFolder("f1_results.txt").toFile().getAbsolutePath(),
                        serviceWorkingFolder.getServiceFolder("f2_results.txt").toFile().getAbsolutePath(),
                        serviceWorkingFolder.getServiceFolder("f3_results.txt").toFile().getAbsolutePath()
                )))
                .thenReturn(serviceComputationFactory.newCompletedComputation(sparkApp));

        ServiceComputation<JacsServiceResult<List<File>>> colorDepthFileSearchComputation = colorDepthFileSearch.process(testService);

        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<List<File>>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        colorDepthFileSearchComputation
                .thenApply(r -> {
                    successful.accept(r);
                    Mockito.verify(sparkCluster).runApp(
                            jarPath,
                            null,
                            0,
                            serviceWorkingFolder.getServiceFolder(JacsServiceFolder.SERVICE_OUTPUT_DIR).toString(),
                            serviceWorkingFolder.getServiceFolder(JacsServiceFolder.SERVICE_ERROR_DIR).toString(),
                            null,
                            (long) SEARCH_INTERVAL_CHECK_IN_MILLIS,
                            SEARCH_TIMEOUT_IN_SECONDS * 1000L,
                            ImmutableList.of(
                                    "-m", "f1", "f2", "f3",
                                    "-i", "s1,s2",
                                    "--maskThresholds", "100", "100", "100",
                                    "--dataThreshold", "100",
                                    "--pixColorFluctuation", "2.0",
                                    "--xyShift", "20",
                                    "--pctPositivePixels", "10.0",
                                    "-o",
                                    serviceWorkingFolder.getServiceFolder("f1_results.txt").toFile().getAbsolutePath(),
                                    serviceWorkingFolder.getServiceFolder("f2_results.txt").toFile().getAbsolutePath(),
                                    serviceWorkingFolder.getServiceFolder("f3_results.txt").toFile().getAbsolutePath()
                            ));
                    Mockito.verify(sparkCluster).stopCluster();
                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    if (exc instanceof NullPointerException) {
                        // this usually occurs if the mocks are not setup correctly
                        fail("Check that the parameters for the mock invocation are as they are expected by the test setup: " + exc.toString());
                    } else {
                        fail(exc.toString());
                    }
                    return null;
                });
    }

    private JacsServiceData createTestServiceData(Number serviceId, String owner) {
        JacsServiceDataBuilder testServiceDataBuilder = new JacsServiceDataBuilder(null)
                .setOwnerKey(owner)
                .setAuthKey(owner)
                .addArgs("-inputFiles", "f1,f2,f3")
                .addArgs("-searchDirs", "s1,s2")
                .addArgs("-maskThresholds", "100").addArgs("100").addArgs("100")
                .addArgs("-numNodes", "9")
                .addArgs("-minWorkerNodes", "3")
                .addArgs("-dataThreshold", "100")
                .addArgs("-pixColorFluctuation", "2.0")
                .addArgs("-xyShift", "20")
                .addArgs("-pctPositivePixels", "10.0")
                ;
        JacsServiceData testServiceData = testServiceDataBuilder
                .setWorkspace(TEST_WORKSPACE)
                .build();
        testServiceData.setId(serviceId);
        testServiceData.setName("colorDepthFileSearch");
        return testServiceData;
    }

}
