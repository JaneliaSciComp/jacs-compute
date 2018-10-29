package org.janelia.jacs2.asyncservice.spark;

import com.google.common.collect.ImmutableList;
import org.janelia.jacs2.asyncservice.common.ComputationTestHelper;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.cluster.ComputeAccounting;
import org.janelia.jacs2.asyncservice.imagesearch.ColorDepthFileSearch;
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
        SparkAppProcessor.class,
        FileUtils.class
})
public class SparkAppProcessorTest {

    private static final String TEST_WORKSPACE = "testSparkWorkspace";
    private static final String DEFAULT_WORKING_DIR = "testWorking";
    private static final int SEARCH_TIMEOUT_IN_SECONDS = 1200;
    private static final int SEARCH_INTERVAL_CHECK_IN_MILLIS = 5000;
    private static final int DEFAULT_NUM_NODES = 6;

    ServiceComputationFactory serviceComputationFactory;
    private JacsServiceDataPersistence jacsServiceDataPersistence;
    private LSFSparkClusterLauncher clusterLauncher;
    private ComputeAccounting clusterAccounting;
    private SparkCluster sparkCluster;
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
        sparkCluster = mock(SparkCluster.class);
        sparkApp = mock(SparkApp.class);
        Mockito.when(sparkApp.isDone()).thenReturn(true);

        sparkAppProcessor = new SparkAppProcessor(serviceComputationFactory,
                jacsServiceDataPersistence,
                DEFAULT_WORKING_DIR,
                clusterLauncher,
                clusterAccounting,
                DEFAULT_NUM_NODES,
                logger);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void process() throws Exception {
        String testAppResource = "testApp";
        List<String> testAppArgs = ImmutableList.of("a1", "a2", "a3");
        int testNumNodes = 12;
        String testDriverMemory = "driverMem";
        String testExecutorMemory = "executorMem";
        Long appIntervalCheckInMillis = null;
        Long appTimeoutInMillis = null;
        String clusterBillingInfo = "clusterBillingInfo";

        JacsServiceData testService = createTestServiceData(1L, testAppResource,
                testAppArgs,
                "test",
                testNumNodes,
                testDriverMemory,
                testExecutorMemory);
        JacsServiceFolder serviceWorkingFolder = new JacsServiceFolder(null, Paths.get(testService.getWorkspace()), testService);

        PowerMockito.mockStatic(Files.class);
        Mockito.when(Files.createDirectories(any(Path.class))).then((Answer<Path>) invocation -> invocation.getArgument(0));
        Mockito.when(clusterAccounting.getComputeAccount(testService)).thenReturn(clusterBillingInfo);

        Mockito.when(clusterLauncher.startCluster(
                testNumNodes,
                serviceWorkingFolder.getServiceFolder(),
                clusterBillingInfo,
                testDriverMemory,
                testExecutorMemory,
                null))
                .thenReturn(serviceComputationFactory.newCompletedComputation(sparkCluster));

        Mockito.when(sparkCluster.runApp(
                testAppResource,
                null,
                0,
                serviceWorkingFolder.getServiceFolder(JacsServiceFolder.SERVICE_OUTPUT_DIR).toString(),
                serviceWorkingFolder.getServiceFolder(JacsServiceFolder.SERVICE_ERROR_DIR).toString(),
                appIntervalCheckInMillis,
                appTimeoutInMillis,
                testAppArgs)
        ).thenReturn(serviceComputationFactory.newCompletedComputation(sparkApp));

        ServiceComputation<JacsServiceResult<Void>> sparkServiceComputation = sparkAppProcessor.process(testService);

        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        sparkServiceComputation
                .thenApply(r -> {
                    successful.accept(r);
                    Mockito.verify(sparkCluster).runApp(
                            testAppResource,
                            null,
                            0,
                            serviceWorkingFolder.getServiceFolder(JacsServiceFolder.SERVICE_OUTPUT_DIR).toString(),
                            serviceWorkingFolder.getServiceFolder(JacsServiceFolder.SERVICE_ERROR_DIR).toString(),
                            appIntervalCheckInMillis,
                            appTimeoutInMillis,
                            testAppArgs);
                    Mockito.verify(sparkCluster).stopCluster();
                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    fail(exc.toString());
                    return null;
                });
    }

    private JacsServiceData createTestServiceData(Number serviceId, String testApp,
                                                  List<String> appArgs,
                                                  String owner,
                                                  int numNodes,
                                                  String driverMemory,
                                                  String executorMemory) {
        JacsServiceDataBuilder testServiceDataBuilder = new JacsServiceDataBuilder(null)
                .setOwnerKey(owner)
                .setAuthKey(owner)
                .addArgs("-appLocation", testApp)
                .addArgs("-appArgs").addArgs(appArgs.stream().reduce((a1, a2) -> a1 + "," + a2).orElse(""))
                .addResource("sparkNumNodes", String.valueOf(numNodes))
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
