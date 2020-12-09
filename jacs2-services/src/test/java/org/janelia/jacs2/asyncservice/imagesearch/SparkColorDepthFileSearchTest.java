package org.janelia.jacs2.asyncservice.imagesearch;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;

import org.janelia.jacs2.asyncservice.common.ComputationTestHelper;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgMatcher;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.spark.SparkAppProcessor;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.testhelpers.ListArgMatcher;
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

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        SparkColorDepthFileSearch.class,
        FileUtils.class
})
public class SparkColorDepthFileSearchTest {

    private static final String TEST_WORKSPACE = "testColorDepthLocalWorkspace";
    private static final String DEFAULT_WORKING_DIR = "testWorking";
    private static final int SEARCH_TIMEOUT_IN_SECONDS = 1200;
    private static final int SEARCH_INTERVAL_CHECK_IN_MILLIS = 5000;
    private static final int DEFAULT_CORES_PER_SPARK_WORKER = 1;
    private static final String DEFAULT_SPARK_HOME = "testSparkHome";

    ServiceComputationFactory serviceComputationFactory;
    private JacsServiceDataPersistence jacsServiceDataPersistence;
    private SparkAppProcessor sparkAppProcessor;
    private final String jarPath = "sparkColorDepthSearch.jar";

    private SparkColorDepthFileSearch sparkColorDepthFileSearch;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        Logger logger = mock(Logger.class);
        serviceComputationFactory = ComputationTestHelper.createTestServiceComputationFactory(logger);
        jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        sparkAppProcessor = mock(SparkAppProcessor.class);

        sparkColorDepthFileSearch = new SparkColorDepthFileSearch(serviceComputationFactory,
                jacsServiceDataPersistence,
                DEFAULT_WORKING_DIR,
                sparkAppProcessor,
                SEARCH_TIMEOUT_IN_SECONDS,
                SEARCH_INTERVAL_CHECK_IN_MILLIS,
                DEFAULT_SPARK_HOME,
                DEFAULT_CORES_PER_SPARK_WORKER,
                jarPath,
                logger);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void process() throws Exception {
        String maskFiles = "m1,m2,m3";
        String targetFiles = "t1,t2,t3";
        String minWorkers = "2";
        String maskThreshold = "50";
        String targetThreshold = "60";
        String pixColorFluctuation = "2.0";
        String xyShift = "2";
        String pctPositivePixels = "1.0";
        String negativeRadius = "20";
        String resultsDir = "cdsresults";
        String numWorkers = "6";
        JacsServiceData testService = createTestServiceData(
                1L,
                "test",
                maskFiles,
                targetFiles,
                minWorkers,
                maskThreshold,
                targetThreshold,
                pixColorFluctuation,
                xyShift,
                pctPositivePixels,
                negativeRadius,
                resultsDir,
                numWorkers);

        Mockito.when(sparkAppProcessor.createServiceData(any(ServiceExecutionContext.class), anyList()))
                .then(invocation -> testService);
        Mockito.when(sparkAppProcessor.process(any(JacsServiceData.class)))
                .then(invocation -> serviceComputationFactory.newCompletedComputation(new JacsServiceResult(testService)));

        PowerMockito.mockStatic(Files.class);
        Mockito.when(Files.createDirectories(any(Path.class))).then((Answer<Path>) invocation -> invocation.getArgument(0));
        Mockito.when(Files.find(any(Path.class), anyInt(), any(BiPredicate.class))).then(invocation -> {
            Path root = invocation.getArgument(0);
            return Stream.of(root.resolve("f1_results.json"));
        });

        ServiceComputation<JacsServiceResult<List<File>>> colorDepthFileSearchComputation = sparkColorDepthFileSearch.process(testService);

        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<List<File>>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        colorDepthFileSearchComputation
                .thenApply(r -> {
                    successful.accept(r);
                    Mockito.verify(sparkAppProcessor).createServiceData(
                            any(ServiceExecutionContext.class),
                            argThat(new ListArgMatcher<>(
                                    Arrays.asList(
                                            new ServiceArgMatcher(new ServiceArg("-appName", "colordepthsearch")),
                                            new ServiceArgMatcher(new ServiceArg("-appLocation", jarPath)),
                                            new ServiceArgMatcher(new ServiceArg("-appEntryPoint", "org.janelia.colormipsearch.cmd.SparkMainEntry")),
                                            new ServiceArgMatcher(new ServiceArg("-appArgs",
                                                    "searchFromJSON," +
                                                            "-m," + maskFiles + "," +
                                                            "-i," + targetFiles + "," +
                                                            "--maskThreshold," + maskThreshold + "," +
                                                            "--dataThreshold," + targetThreshold + "," +
                                                            "--pixColorFluctuation," + pixColorFluctuation + "," +
                                                            "--xyShift," + xyShift + "," +
                                                            "--mirrorMask," +
                                                            "--pctPositivePixels," + pctPositivePixels + "," +
                                                            "--negativeRadius," + negativeRadius + "," +
                                                            "--with-grad-scores," +
                                                            "--outputDir," + resultsDir))
                                    )
                            ))
                    );
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

    private JacsServiceData createTestServiceData(Number serviceId,
                                                  String owner,
                                                  String maskFiles,
                                                  String targetFiles,
                                                  String minWorkers,
                                                  String maskThreshold,
                                                  String targetThreshold,
                                                  String pixColorFluctuation,
                                                  String xyShift,
                                                  String pctPositivePixels,
                                                  String negativeRadius,
                                                  String resultsDir,
                                                  String numWorkers) {
        JacsServiceDataBuilder testServiceDataBuilder = new JacsServiceDataBuilder(null)
                .setOwnerKey(owner)
                .setAuthKey(owner)
                .addArgs("-masksFiles", maskFiles)
                .addArgs("-targetsFiles", targetFiles)
                .addArgs("-minWorkerNodes", minWorkers)
                .addArgs("-maskThreshold", maskThreshold)
                .addArgs("-dataThreshold", targetThreshold)
                .addArgs("-pixColorFluctuation", pixColorFluctuation)
                .addArgs("-xyShift", xyShift)
                .addArgs("-mirrorMask")
                .addArgs("-pctPositivePixels", pctPositivePixels)
                .addArgs("-negativeRadius", negativeRadius)
                .addArgs("-cdMatchesDir", resultsDir)
                .addArgs("-numWorkers", numWorkers)
                .addArgs("-withGradientScores")
                ;
        JacsServiceData testServiceData = testServiceDataBuilder
                .setWorkspace(TEST_WORKSPACE)
                .build();
        testServiceData.setId(serviceId);
        testServiceData.setName("colorDepthFileSearch");
        return testServiceData;
    }

}
