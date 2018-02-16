package org.janelia.jacs2.asyncservice.imagesearch;

import org.janelia.jacs2.asyncservice.common.ComputationTestUtils;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.spark.SparkApp;
import org.janelia.jacs2.asyncservice.common.spark.SparkCluster;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import javax.enterprise.inject.Instance;
import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class ColorDepthFileSearchTest {

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

    @Before
    public void setUp() throws Exception {
        jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        jacsJobInstanceInfoDao = mock(JacsJobInstanceInfoDao.class);
        clusterSource = mock(Instance.class);
        cluster = mock(SparkCluster.class);
        sparkApp = mock(SparkApp.class);
        Mockito.when(clusterSource.get()).thenReturn(cluster);
        Mockito.when(cluster.isReady()).thenReturn(true);
        Mockito.when(sparkApp.isDone()).thenReturn(true);
        Logger logger = mock(Logger.class);
        ServiceComputationFactory serviceComputationFactory = ComputationTestUtils.createTestServiceComputationFactory(logger);

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

    @Test
    public void process() throws Exception {
        JacsServiceData testService = createTestServiceData(1L, "test");
        JacsServiceFolder serviceWorkingFolder = new JacsServiceFolder(null, Paths.get(testService.getWorkspace()), testService);
        Mockito.when(cluster.runApp(null, null,
                "-m", "f1", "f2", "f3", "-i", "s1,s2",
                "--maskThresholds", "100", "100", "100",
                "--dataThreshold", "100",
                "--pixColorFluctuation", "2.0",
                "--pctPositivePixels", "10.0",
                "-o",
                serviceWorkingFolder.getServiceFolder("f1_results.txt").toFile().getAbsolutePath(),
                serviceWorkingFolder.getServiceFolder("f2_results.txt").toFile().getAbsolutePath(),
                serviceWorkingFolder.getServiceFolder("f3_results.txt").toFile().getAbsolutePath())).thenReturn(sparkApp);

        ServiceComputation<JacsServiceResult<List<File>>> colorDepthFileSearchComputation = colorDepthFileSearch.process(testService);

        Consumer successful = mock(Consumer.class);
        Consumer failure = mock(Consumer.class);
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
                .addArg("-inputFiles", "f1,f2,f3")
                .addArg("-searchDirs", "s1,s2")
                .addArg("-maskThresholds", "100").addArg("100").addArg("100")
                .addArg("-numNodes", "9")
                .addArg("-dataThreshold", "100")
                .addArg("-pixColorFluctuation", "2.0")
                .addArg("-pctPositivePixels", "10.0")
                ;
        JacsServiceData testServiceData = testServiceDataBuilder
                .setWorkspace("testlocal")
                .build();
        testServiceData.setId(serviceId);
        testServiceData.setName("colorDepthFileSearch");
        return testServiceData;
    }

}
