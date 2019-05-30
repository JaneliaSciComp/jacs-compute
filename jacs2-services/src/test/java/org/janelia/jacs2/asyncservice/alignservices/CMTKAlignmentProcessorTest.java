package org.janelia.jacs2.asyncservice.alignservices;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.ComputationTestHelper;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgMatcher;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceProcessorTestHelper;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.testhelpers.ListArgMatcher;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.janelia.model.service.JacsServiceState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CMTKAlignmentProcessorTest {
    private static final Long TEST_SERVICE_ID = 1L;
    private static final String TEST_IMAGES_DIR = "src/test/resources/testdata/cmtkalign/images/";
    private static final String TEST_WORKING_DIR = "testWorkingDir";

    private SingleCMTKAlignmentProcessor singleCMTKAlignmentProcessor;
    private CMTKAlignmentProcessor cmtkAlignmentProcessor;
    private Path testDirectory;

    @Before
    public void setUp() throws IOException {
        Logger logger = mock(Logger.class);

        ServiceComputationFactory computationFactory = ComputationTestHelper.createTestServiceComputationFactory(logger);

        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        singleCMTKAlignmentProcessor = mock(SingleCMTKAlignmentProcessor.class);

        when(jacsServiceDataPersistence.findById(any(Number.class))).then(invocation -> {
            JacsServiceData sd = new JacsServiceData();
            sd.setId(invocation.getArgument(0));
            sd.setState(JacsServiceState.SUCCESSFUL);
            return sd;
        });

        when(jacsServiceDataPersistence.createServiceIfNotFound(any(JacsServiceData.class))).then(invocation -> {
            JacsServiceData jacsServiceData = invocation.getArgument(0);
            jacsServiceData.setId(TEST_SERVICE_ID);
            jacsServiceData.setState(JacsServiceState.SUCCESSFUL); // mark the service as completed otherwise the computation doesn't return
            return jacsServiceData;
        });

        ServiceProcessorTestHelper.prepareServiceProcessorMetadataAsRealCall(singleCMTKAlignmentProcessor);

        cmtkAlignmentProcessor = new CMTKAlignmentProcessor(computationFactory,
                jacsServiceDataPersistence,
                TEST_WORKING_DIR,
                singleCMTKAlignmentProcessor,
                logger);

        testDirectory = Files.createTempDirectory("testcmtk");
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deletePath(testDirectory);
    }

    @Test
    public void processInputImages() {
        List<String> inputImages = ImmutableList.of(
                "GMR_09C10_AE_01_23-fA01b_C100120_20100120125311859_01.nrrd",
                "GMR_09C10_AE_01_23-fA01b_C100120_20100120125311859_02.nrrd",
                "GMR_09C11_AE_01_57-fA01b_C101214_20101214100952734_01.nrrd",
                "GMR_09C11_AE_01_57-fA01b_C101214_20101214100952734_02.nrrd",
                "GMR_09D12_AE_01_03-fA01b_C110218_20110218103433625_01.nrrd",
                "GMR_09D12_AE_01_03-fA01b_C110218_20110218103433625_02.nrrd"
        );
        String outputDir = testDirectory.toString();

        @SuppressWarnings("unchecked")
        ServiceResultHandler<CMTKAlignmentResultFiles> singleCmtkAlignmentResultHandler = mock(ServiceResultHandler.class);
        when(singleCMTKAlignmentProcessor.getResultHandler()).thenReturn(singleCmtkAlignmentResultHandler);
        when(singleCmtkAlignmentResultHandler.getServiceDataResult(any(JacsServiceData.class))).thenReturn(new CMTKAlignmentResultFiles());

        JacsServiceData testServiceData = createTestServiceData(TEST_SERVICE_ID, inputImages, outputDir);
        ServiceComputation<JacsServiceResult<List<String>>> cmtkAlignment = cmtkAlignmentProcessor.process(testServiceData);
        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<List<String>>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        cmtkAlignment
                .thenApply(r -> {
                    successful.accept(r);
                    verify(singleCMTKAlignmentProcessor).createServiceData(
                            any(ServiceExecutionContext.class),
                            argThat(new ListArgMatcher<>(
                                    ImmutableList.of(
                                            new ServiceArgMatcher(new ServiceArg("-inputDir", outputDir + File.separatorChar + "GMR_09C10_AE_01_23-fA01b_C100120_20100120125311859" + File.separatorChar + "images")),
                                            new ServiceArgMatcher(new ServiceArg("-outputDir", outputDir + File.separatorChar + "GMR_09C10_AE_01_23-fA01b_C100120_20100120125311859")),
                                            new ServiceArgMatcher(new ServiceArg("-template", "")),
                                            new ServiceArgMatcher(new ServiceArg("-a", "true")),
                                            new ServiceArgMatcher(new ServiceArg("-w", "true")),
                                            new ServiceArgMatcher(new ServiceArg("-r", "0102030405")),
                                            new ServiceArgMatcher(new ServiceArg("-X", "26")),
                                            new ServiceArgMatcher(new ServiceArg("-C", "8")),
                                            new ServiceArgMatcher(new ServiceArg("-G", "80")),
                                            new ServiceArgMatcher(new ServiceArg("-R", "4")),
                                            new ServiceArgMatcher(new ServiceArg("-A", "--accuracy 0.8")),
                                            new ServiceArgMatcher(new ServiceArg("-W", "--accuracy 0.8")),
                                            new ServiceArgMatcher(new ServiceArg("-nthreads", "")),
                                            new ServiceArgMatcher(new ServiceArg("-verbose", false))
                                    ))
                            )
                    );
                    verify(singleCMTKAlignmentProcessor).createServiceData(
                            any(ServiceExecutionContext.class),
                            argThat(new ListArgMatcher<>(
                                    ImmutableList.of(
                                            new ServiceArgMatcher(new ServiceArg("-inputDir", outputDir + File.separatorChar + "GMR_09C11_AE_01_57-fA01b_C101214_20101214100952734" + File.separatorChar + "images")),
                                            new ServiceArgMatcher(new ServiceArg("-outputDir", outputDir + File.separatorChar + "GMR_09C11_AE_01_57-fA01b_C101214_20101214100952734")),
                                            new ServiceArgMatcher(new ServiceArg("-template", "")),
                                            new ServiceArgMatcher(new ServiceArg("-a", "true")),
                                            new ServiceArgMatcher(new ServiceArg("-w", "true")),
                                            new ServiceArgMatcher(new ServiceArg("-r", "0102030405")),
                                            new ServiceArgMatcher(new ServiceArg("-X", "26")),
                                            new ServiceArgMatcher(new ServiceArg("-C", "8")),
                                            new ServiceArgMatcher(new ServiceArg("-G", "80")),
                                            new ServiceArgMatcher(new ServiceArg("-R", "4")),
                                            new ServiceArgMatcher(new ServiceArg("-A", "--accuracy 0.8")),
                                            new ServiceArgMatcher(new ServiceArg("-W", "--accuracy 0.8")),
                                            new ServiceArgMatcher(new ServiceArg("-nthreads", "")),
                                            new ServiceArgMatcher(new ServiceArg("-verbose", false))
                                    ))
                            )

                    );
                    verify(singleCMTKAlignmentProcessor).createServiceData(
                            any(ServiceExecutionContext.class),
                            argThat(new ListArgMatcher<>(
                                    ImmutableList.of(
                                            new ServiceArgMatcher(new ServiceArg("-inputDir", outputDir + File.separatorChar + "GMR_09D12_AE_01_03-fA01b_C110218_20110218103433625" + File.separatorChar + "images")),
                                            new ServiceArgMatcher(new ServiceArg("-outputDir", outputDir + File.separatorChar + "GMR_09D12_AE_01_03-fA01b_C110218_20110218103433625")),
                                            new ServiceArgMatcher(new ServiceArg("-template", "")),
                                            new ServiceArgMatcher(new ServiceArg("-a", "true")),
                                            new ServiceArgMatcher(new ServiceArg("-w", "true")),
                                            new ServiceArgMatcher(new ServiceArg("-r", "0102030405")),
                                            new ServiceArgMatcher(new ServiceArg("-X", "26")),
                                            new ServiceArgMatcher(new ServiceArg("-C", "8")),
                                            new ServiceArgMatcher(new ServiceArg("-G", "80")),
                                            new ServiceArgMatcher(new ServiceArg("-R", "4")),
                                            new ServiceArgMatcher(new ServiceArg("-A", "--accuracy 0.8")),
                                            new ServiceArgMatcher(new ServiceArg("-W", "--accuracy 0.8")),
                                            new ServiceArgMatcher(new ServiceArg("-nthreads", "")),
                                            new ServiceArgMatcher(new ServiceArg("-verbose", false))
                                    ))
                            )
                    );
                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    fail(exc.toString());
                    return null;
                });
    }

    private JacsServiceData createTestServiceData(Long serviceId, List<String> inputImages, String outputDir) {
        JacsServiceDataBuilder testServiceDataBuilder = new JacsServiceDataBuilder(null)
                .setOwnerKey("testOwner")
                .addArgs("-inputImages", inputImages
                        .stream()
                        .map(inputImageName -> TEST_IMAGES_DIR + inputImageName)
                        .reduce("", (i1, i2) -> StringUtils.isBlank(i1) ? i2 : i1 + "," + i2))
                .addArgs("-outputDir", outputDir)
                .setWorkspace(TEST_WORKING_DIR);

        JacsServiceData testServiceData = testServiceDataBuilder.build();
        testServiceData.setId(serviceId);
        return testServiceData;
    }

}
