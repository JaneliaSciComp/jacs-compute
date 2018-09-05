package org.janelia.jacs2.asyncservice.imagesearch;

import org.janelia.jacs2.asyncservice.common.ComputationTestHelper;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.spark.SparkApp;
import org.janelia.jacs2.asyncservice.common.spark.SparkCluster;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
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

import javax.enterprise.inject.Instance;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

    private JacsServiceDataPersistence jacsServiceDataPersistence;
    private JacsJobInstanceInfoDao jacsJobInstanceInfoDao;
    private Instance<SparkCluster> clusterSource;
    private SparkCluster cluster;
    private SparkApp sparkApp;
    private String defaultWorkingDir = "testWorking";
    private int clusterStartTimeoutInSeconds= 3600;
    private int searchTimeoutInSeconds= 1200;
    private int clusterIntervalCheckInMillis= 2000;
    private int searchIntervalCheckInMillis= 5000;
    private Integer numNodes= 6;
    private String jarPath;

    private ColorDepthFileSearch colorDepthFileSearch;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        jacsJobInstanceInfoDao = mock(JacsJobInstanceInfoDao.class);
        clusterSource = mock(Instance.class);
        cluster = mock(SparkCluster.class);
        sparkApp = mock(SparkApp.class);
        Mockito.when(clusterSource.get()).thenReturn(cluster);
        Mockito.when(cluster.isReady()).thenReturn(true);
        Mockito.when(sparkApp.isDone()).thenReturn(true);
        Logger logger = mock(Logger.class);
        ServiceComputationFactory serviceComputationFactory = ComputationTestHelper.createTestServiceComputationFactory(logger);

        colorDepthFileSearch = new ColorDepthFileSearch(serviceComputationFactory,
                jacsServiceDataPersistence,
                defaultWorkingDir,
                clusterSource,
                clusterStartTimeoutInSeconds,
                searchTimeoutInSeconds,
                clusterIntervalCheckInMillis,
                searchIntervalCheckInMillis,
                numNodes,
                jarPath,
                logger);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void process() throws Exception {
        JacsServiceData testService = createTestServiceData(1L, "test");
        JacsServiceFolder serviceWorkingFolder = new JacsServiceFolder(null, Paths.get(testService.getWorkspace()), testService);

        PowerMockito.mockStatic(Files.class);
        Mockito.when(Files.createDirectories(any(Path.class))).then((Answer<Path>) invocation -> invocation.getArgument(0));
        Mockito.when(Files.find(any(Path.class), anyInt(), any(BiPredicate.class))).then(invocation -> {
            Path root = invocation.getArgument(0);
            return Stream.of(root.resolve("f1_results.txt"));
        });

        Mockito.when(cluster.runApp(
                null,
                null,
                new String[] {
                        "-m", "f1", "f2", "f3", "-i", "s1,s2",
                        "--maskThresholds", "100", "100", "100",
                        "--dataThreshold", "100",
                        "--pixColorFluctuation", "2.0",
                        "--pctPositivePixels", "10.0",
                        "-o",
                        serviceWorkingFolder.getServiceFolder("f1_results.txt").toFile().getAbsolutePath(),
                        serviceWorkingFolder.getServiceFolder("f2_results.txt").toFile().getAbsolutePath(),
                        serviceWorkingFolder.getServiceFolder("f3_results.txt").toFile().getAbsolutePath()
                }))
                .thenReturn(sparkApp);

        ServiceComputation<JacsServiceResult<List<File>>> colorDepthFileSearchComputation = colorDepthFileSearch.process(testService);

        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<List<File>>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        colorDepthFileSearchComputation
                .thenApply(r -> {
                    successful.accept(r);
                    Mockito.verify(cluster).stopCluster();
                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    fail(exc.toString());
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
                .addArgs("-dataThreshold", "100")
                .addArgs("-pixColorFluctuation", "2.0")
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
