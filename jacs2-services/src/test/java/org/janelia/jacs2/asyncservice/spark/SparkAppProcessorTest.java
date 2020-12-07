package org.janelia.jacs2.asyncservice.spark;

import com.google.common.collect.ImmutableList;
import org.janelia.jacs2.asyncservice.common.ComputationTestHelper;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.cluster.ComputeAccounting;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.StrPropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.janelia.model.service.ProcessingLocation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        SparkAppProcessor.class,
        FileUtils.class
})
public class SparkAppProcessorTest {

    private static final String TEST_WORKSPACE = "testSparkWorkspace";
    private static final String DEFAULT_WORKING_DIR = "testWorking";
    private static final String DEFAULT_SPARK_HOME = "testSparkHome";
    private static final String DEFAULT_SPARK_DRIVER_MEMORY = "1g";
    private static final int DEFAULT_CORES_PER_SPARK_EXECUTOR = 3;
    private static final int DEFAULT_MEMORY_PER_CORE_IN_GB = 15;
    private static final int DEFAULT_SPARK_DURATION_MINS = 60;
    private static final String DEFAULT_SPARK_LOG_CONFIG = "logconfig";
    private static final String HADOOP_HOME_DIR = "hadoopHome";

    ServiceComputationFactory serviceComputationFactory;
    private JacsServiceDataPersistence jacsServiceDataPersistence;
    private LSFSparkClusterLauncher clusterLauncher;
    private ComputeAccounting clusterAccounting;
    private LSFSparkCluster sparkCluster;
    private SparkApp sparkApp;

    private SparkAppProcessor sparkAppProcessor;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        Logger logger = mock(Logger.class);
        serviceComputationFactory = ComputationTestHelper.createTestServiceComputationFactory(logger);
        jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        clusterLauncher = mock(LSFSparkClusterLauncher.class);
        clusterAccounting = mock(ComputeAccounting.class);
        sparkCluster = mock(LSFSparkCluster.class);
        sparkApp = mock(SparkApp.class);
        Mockito.when(sparkApp.isDone()).thenReturn(true);

        sparkAppProcessor = new SparkAppProcessor(serviceComputationFactory,
                jacsServiceDataPersistence,
                DEFAULT_WORKING_DIR,
                clusterLauncher,
                clusterAccounting,
                DEFAULT_SPARK_HOME,
                DEFAULT_SPARK_DRIVER_MEMORY,
                DEFAULT_CORES_PER_SPARK_EXECUTOR,
                DEFAULT_MEMORY_PER_CORE_IN_GB,
                DEFAULT_SPARK_DURATION_MINS,
                DEFAULT_SPARK_LOG_CONFIG,
                HADOOP_HOME_DIR,
                logger);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void processWithLocalSparkDriver() throws Exception {
        String testAppResource = "testApp";
        List<String> testAppArgs = ImmutableList.of("a1", "a2", "a3");
        int testNumNodes = 12;
        int testMinRequiredWorkers = 4;
        String testDriverMemory = "driverMem";
        String testExecutorMemory = "executorMem";
        String clusterBillingInfo = "clusterBillingInfo";

        JacsServiceData testService = createTestServiceData(1L, testAppResource,
                testAppArgs,
                "test",
                testNumNodes,
                testMinRequiredWorkers,
                testDriverMemory,
                testExecutorMemory,
                ProcessingLocation.LOCAL);
        JacsServiceFolder serviceWorkingFolder = new JacsServiceFolder(null, Paths.get(testService.getWorkspace()), testService);
        Path serviceOutputPath = serviceWorkingFolder.getServiceFolder(JacsServiceFolder.SERVICE_OUTPUT_DIR);
        Path serviceErrorPath = serviceWorkingFolder.getServiceFolder(JacsServiceFolder.SERVICE_ERROR_DIR);

        PowerMockito.mockStatic(Files.class);
        Mockito.when(Files.createDirectories(any(Path.class))).then((Answer<Path>) invocation -> invocation.getArgument(0));
        Mockito.when(clusterAccounting.getComputeAccount(testService)).thenReturn(clusterBillingInfo);

        Mockito.when(clusterLauncher.startCluster(
                testAppResource,
                DEFAULT_SPARK_HOME,
                testNumNodes,
                DEFAULT_CORES_PER_SPARK_EXECUTOR,
                testMinRequiredWorkers,
                serviceWorkingFolder.getServiceFolder(),
                serviceOutputPath,
                serviceErrorPath,
                clusterBillingInfo,
                DEFAULT_SPARK_DURATION_MINS))
                .thenReturn(serviceComputationFactory.newCompletedComputation(sparkCluster));

        SparkClusterInfo testClusterInfo = new SparkClusterInfo(1L, 2L, "spark://sparkmasterhost:7077");
        Mockito.when(sparkCluster.getSparkClusterInfo())
                .thenReturn(testClusterInfo);
        Map<String, String> appResources = SparkAppResourceHelper.sparkAppResourceBuilder()
                .sparkHome(DEFAULT_SPARK_HOME)
                .sparkDriverMemory(DEFAULT_SPARK_DRIVER_MEMORY)
                .sparkWorkerCores(DEFAULT_CORES_PER_SPARK_EXECUTOR)
                .sparkWorkerMemoryPerCoreInGB(DEFAULT_MEMORY_PER_CORE_IN_GB)
                .sparkAppTimeoutInMillis(DEFAULT_SPARK_DURATION_MINS * 60L * 1000L)
                .sparkLogConfigFile(DEFAULT_SPARK_LOG_CONFIG)
                .hadoopHome(HADOOP_HOME_DIR)
                .addAll(testService.getResources())
                .build();
        SparkDriverRunner<? extends SparkApp> sparkDriverRunner = mock(SparkDriverRunner.class);
        Mockito.when(clusterLauncher.getLocalDriverRunner()).then(invocation -> sparkDriverRunner);
        Mockito.when(sparkDriverRunner.startSparkApp(
                testClusterInfo,
                testAppResource,
                null,
                testAppArgs,
                serviceWorkingFolder.getServiceFolder(JacsServiceFolder.SERVICE_OUTPUT_DIR).toString(),
                serviceWorkingFolder.getServiceFolder(JacsServiceFolder.SERVICE_ERROR_DIR).toString(),
                appResources)
        ).then(invocation -> sparkApp);

        ServiceComputation<JacsServiceResult<Void>> sparkServiceComputation = sparkAppProcessor.process(testService);

        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        sparkServiceComputation
                .thenApply(r -> {
                    successful.accept(r);
                    Mockito.verify(sparkDriverRunner).startSparkApp(
                            testClusterInfo,
                            testAppResource,
                            null,
                            testAppArgs,
                            serviceWorkingFolder.getServiceFolder(JacsServiceFolder.SERVICE_OUTPUT_DIR).toString(),
                            serviceWorkingFolder.getServiceFolder(JacsServiceFolder.SERVICE_ERROR_DIR).toString(),
                            appResources);
                    Mockito.verify(sparkCluster).stopCluster();
                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    fail(exc.toString());
                    return null;
                });
    }

    private JacsServiceData createTestServiceData(Number serviceId,
                                                  String testApp,
                                                  List<String> appArgs,
                                                  String owner,
                                                  int numNodes,
                                                  int minRequiredWorkers,
                                                  String driverMemory,
                                                  String executorMemory,
                                                  ProcessingLocation processingLocation) {
        JacsServiceDataBuilder testServiceDataBuilder = new JacsServiceDataBuilder(null)
                .setOwnerKey(owner)
                .setAuthKey(owner)
                .setProcessingLocation(processingLocation)
                .addArgs("-appName", testApp)
                .addArgs("-appLocation", testApp)
                .addArgs("-appArgs").addArgs(appArgs.stream().reduce((a1, a2) -> a1 + "," + a2).orElse(""))
                .addResource("sparkAppStackSize", "128M")
                .addResource("sparkNumNodes", String.valueOf(numNodes))
                .addResource("minSparkWorkers", String.valueOf(minRequiredWorkers))
                .addResource("sparkDriverMemory", driverMemory)
                .addResource("sparkExecutorMemory", executorMemory)
                ;
        JacsServiceData testServiceData = testServiceDataBuilder
                .setWorkspace(TEST_WORKSPACE)
                .build();
        testServiceData.setId(serviceId);
        testServiceData.setName("sparkProcessor");
        return testServiceData;
    }

}
