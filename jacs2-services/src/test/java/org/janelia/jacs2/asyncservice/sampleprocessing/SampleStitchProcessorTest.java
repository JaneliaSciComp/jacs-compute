package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.ComputationTestUtils;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgMatcher;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.imageservices.MIPGenerationProcessor;
import org.janelia.jacs2.asyncservice.imageservices.StitchAndBlendResult;
import org.janelia.jacs2.asyncservice.imageservices.Vaa3dStitchAndBlendProcessor;
import org.janelia.jacs2.asyncservice.imageservices.tools.ChannelComponents;
import org.janelia.model.access.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.janelia.model.service.JacsServiceState;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.File;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SampleStitchProcessorTest {
    private static final String TEST_RESULTS_ID = "23";
    public static final String TEST_SIGNAL_CHANNELS = "0 1";
    private static final String TEST_REFERENCE_CHANNELS = "3";

    private MergeAndGroupSampleTilePairsProcessor mergeAndGroupSampleTilePairsProcessor;
    private Vaa3dStitchAndBlendProcessor vaa3dStitchAndBlendProcessor;
    private MIPGenerationProcessor mipGenerationProcessor;
    private SampleStitchProcessor sampleStitchProcessor;

    @Before
    public void setUp() {
        Logger logger = mock(Logger.class);

        ServiceComputationFactory computationFactory = ComputationTestUtils.createTestServiceComputationFactory(logger);
        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        TimebasedIdentifierGenerator idGenerator = mock(TimebasedIdentifierGenerator.class);

        mergeAndGroupSampleTilePairsProcessor = mock(MergeAndGroupSampleTilePairsProcessor.class);
        vaa3dStitchAndBlendProcessor = mock(Vaa3dStitchAndBlendProcessor.class);
        mipGenerationProcessor = mock(MIPGenerationProcessor.class);

        when(jacsServiceDataPersistence.findById(any(Number.class))).then(invocation -> {
            JacsServiceData sd = new JacsServiceData();
            sd.setId(invocation.getArgument(0));
            sd.setState(JacsServiceState.SUCCESSFUL);
            return sd;
        });

        when(jacsServiceDataPersistence.createServiceIfNotFound(any(JacsServiceData.class))).then(invocation -> {
            JacsServiceData jacsServiceData = invocation.getArgument(0);
            jacsServiceData.setId(SampleProcessorTestUtils.TEST_SERVICE_ID);
            jacsServiceData.setState(JacsServiceState.SUCCESSFUL); // mark the service as completed otherwise the computation doesn't return
            return jacsServiceData;
        });

        when(idGenerator.generateId()).thenReturn(SampleProcessorTestUtils.TEST_SERVICE_ID);

        when(mergeAndGroupSampleTilePairsProcessor.getMetadata()).thenCallRealMethod();
        when(mergeAndGroupSampleTilePairsProcessor.createServiceData(any(ServiceExecutionContext.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class)
                )
        ).thenCallRealMethod();

        when(vaa3dStitchAndBlendProcessor.getMetadata()).thenCallRealMethod();
        when(vaa3dStitchAndBlendProcessor.createServiceData(any(ServiceExecutionContext.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class)
                )
        ).thenCallRealMethod();

        when(mipGenerationProcessor.getMetadata()).thenCallRealMethod();
        when(mipGenerationProcessor.createServiceData(any(ServiceExecutionContext.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class)
                )
        ).thenCallRealMethod();

        sampleStitchProcessor = new SampleStitchProcessor(computationFactory,
                jacsServiceDataPersistence,
                SampleProcessorTestUtils.TEST_WORKING_DIR,
                mergeAndGroupSampleTilePairsProcessor,
                vaa3dStitchAndBlendProcessor,
                mipGenerationProcessor,
                idGenerator,
                logger);
    }

    @Test
    public void processThatRequireStitching() {
        String area = "brain";
        String objective = "40x";
        String mergeAlgorithm = "FLYLIGHT_ORDERED";
        String channelDyeSpec = "reference=Alexa Fluor 488,Cy2;" +
                "membrane_ha=,ATTO 647,Alexa Fluor 633,Alexa Fluor 647,Cy5;" +
                "membrane_v5=Alexa Fluor 546,Alexa Fluor 555,Alexa Fluor 568,DY-547;" +
                "membrane_flag=Alexa Fluor 594";
        String outputChannelOrder = "membrane_ha,membrane_v5,membrane_flag,reference";
        Long testSampleId = SampleProcessorTestUtils.TEST_SAMPLE_ID;


        ServiceResultHandler<List<SampleAreaResult>> mergeAndGroupSampleTilePairsResultHandler = mock(ServiceResultHandler.class);
        when(mergeAndGroupSampleTilePairsProcessor.getResultHandler()).thenReturn(mergeAndGroupSampleTilePairsResultHandler);
        when(mergeAndGroupSampleTilePairsResultHandler.getServiceDataResult(any(JacsServiceData.class))).then(invocation -> {
            return ImmutableList.of(
                    createSampleAreaResult(
                            testSampleId,
                            objective,
                            area,
                            ImmutableList.of(
                                    createTilePairResult("t1", "rt1"),
                                    createTilePairResult("t2", "rt2")
                            ),
                            ImmutableList.of(
                                    createTilePairResult("tm1", "rtm1"),
                                    createTilePairResult("tm2", "rtm2")
                            )
                    )
            );
        });

        ServiceResultHandler<StitchAndBlendResult> vaa3dStitchAndBlendResultHandler = mock(ServiceResultHandler.class);
        when(vaa3dStitchAndBlendProcessor.getResultHandler()).thenReturn(vaa3dStitchAndBlendResultHandler);
        when(vaa3dStitchAndBlendResultHandler.getServiceDataResult(any(JacsServiceData.class))).then(invocation -> {
            StitchAndBlendResult stitchAndBlendResult = new StitchAndBlendResult();
            stitchAndBlendResult.setStitchedFile(new File("stitched"));
            stitchAndBlendResult.setStitchedImageInfoFile(new File("stitchInfo"));
            return stitchAndBlendResult;
        });

        ServiceResultHandler<List<File>> mipGenerationResultHandler = mock(ServiceResultHandler.class);
        when(mipGenerationProcessor.getResultHandler()).thenReturn(mipGenerationResultHandler);
        when(mipGenerationResultHandler.getServiceDataResult(any(JacsServiceData.class))).then(invocation -> {
            return ImmutableList.of(new File("mip1"), new File("mip2"));
        });

        JacsServiceData testServiceData = createTestServiceData(1L,
                testSampleId,
                area,
                objective,
                mergeAlgorithm,
                channelDyeSpec,
                outputChannelOrder,
                true
        );

        ServiceComputation<JacsServiceResult<SampleResult>> stitchProcessing = sampleStitchProcessor.process(testServiceData);
        Consumer successful = mock(Consumer.class);
        Consumer failure = mock(Consumer.class);
        stitchProcessing
                .thenApply(r -> {
                    successful.accept(r);

                    verify(mergeAndGroupSampleTilePairsProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", testSampleId))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-area", area))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleResultsId", TEST_RESULTS_ID))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataRootDir", SampleProcessorTestUtils.TEST_WORKING_DIR))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleLsmsSubDir", "lsms"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleSummarySubDir", "summary"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleSitchingSubDir", "stitching"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-mergeAlgorithm", mergeAlgorithm))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-channelDyeSpec", channelDyeSpec))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-outputChannelOrder", outputChannelOrder)))
                    );

                    verify(vaa3dStitchAndBlendProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-inputDir", new File(SampleProcessorTestUtils.TEST_WORKING_DIR + "/" + area + "/group").getAbsolutePath()))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-outputFile", new File(SampleProcessorTestUtils.TEST_WORKING_DIR + "/" + area + "/stitch/" + "stitched-" + objective + area + ".v3draw").getAbsolutePath()))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-refchannel", TEST_REFERENCE_CHANNELS)))
                    );

                    verify(mipGenerationProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-inputFile", new File("stitched").getAbsolutePath()))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-outputDir", new File(SampleProcessorTestUtils.TEST_WORKING_DIR + "/" + area + "/mips").getAbsolutePath()))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-signalChannels", TEST_SIGNAL_CHANNELS))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-referenceChannel", TEST_REFERENCE_CHANNELS))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-imgFormat", "png")))
                    );

                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    fail(exc.toString());
                    return null;
                });

        verify(failure, never()).accept(any());
        verify(successful).accept(any());
    }

    @Test
    public void processWithStitchingAndNoMips() {
        String area = "brain";
        String objective = "40x";
        String mergeAlgorithm = "FLYLIGHT_ORDERED";
        String channelDyeSpec = "reference=Alexa Fluor 488,Cy2;" +
                "membrane_ha=,ATTO 647,Alexa Fluor 633,Alexa Fluor 647,Cy5;" +
                "membrane_v5=Alexa Fluor 546,Alexa Fluor 555,Alexa Fluor 568,DY-547;" +
                "membrane_flag=Alexa Fluor 594";
        String outputChannelOrder = "membrane_ha,membrane_v5,membrane_flag,reference";
        Long testSampleId = SampleProcessorTestUtils.TEST_SAMPLE_ID;

        ServiceResultHandler<List<SampleAreaResult>> mergeAndGroupSampleTilePairsResultHandler = mock(ServiceResultHandler.class);
        when(mergeAndGroupSampleTilePairsProcessor.getResultHandler()).thenReturn(mergeAndGroupSampleTilePairsResultHandler);
        when(mergeAndGroupSampleTilePairsResultHandler.getServiceDataResult(any(JacsServiceData.class))).then(invocation -> {
            return ImmutableList.of(
                    createSampleAreaResult(
                            testSampleId,
                            objective,
                            area,
                            ImmutableList.of(
                                    createTilePairResult("t1", "rt1"),
                                    createTilePairResult("t2", "rt2")
                            ),
                            ImmutableList.of(
                                    createTilePairResult("tm1", "rtm1"),
                                    createTilePairResult("tm2", "rtm2")
                            )
                    )
            );
        });

        ServiceResultHandler<StitchAndBlendResult> vaa3dStitchAndBlendResultHandler = mock(ServiceResultHandler.class);
        when(vaa3dStitchAndBlendProcessor.getResultHandler()).thenReturn(vaa3dStitchAndBlendResultHandler);
        when(vaa3dStitchAndBlendResultHandler.getServiceDataResult(any(JacsServiceData.class))).then(invocation -> {
            StitchAndBlendResult stitchAndBlendResult = new StitchAndBlendResult();
            stitchAndBlendResult.setStitchedFile(new File("stitched"));
            stitchAndBlendResult.setStitchedImageInfoFile(new File("stitchInfo"));
            return stitchAndBlendResult;
        });

        ServiceResultHandler<List<File>> mipGenerationResultHandler = mock(ServiceResultHandler.class);
        when(mipGenerationProcessor.getResultHandler()).thenReturn(mipGenerationResultHandler);
        when(mipGenerationResultHandler.getServiceDataResult(any(JacsServiceData.class))).then(invocation -> {
            return ImmutableList.of(new File("mip1"), new File("mip2"));
        });

        JacsServiceData testServiceData = createTestServiceData(1L,
                testSampleId,
                area,
                objective,
                mergeAlgorithm,
                channelDyeSpec,
                outputChannelOrder,
                false
        );

        ServiceComputation<JacsServiceResult<SampleResult>> stitchProcessing = sampleStitchProcessor.process(testServiceData);
        Consumer successful = mock(Consumer.class);
        Consumer failure = mock(Consumer.class);
        stitchProcessing
                .thenApply(r -> {
                    successful.accept(r);

                    verify(mergeAndGroupSampleTilePairsProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", testSampleId))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-area", area))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleResultsId", TEST_RESULTS_ID))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataRootDir", SampleProcessorTestUtils.TEST_WORKING_DIR))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleLsmsSubDir", "lsms"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleSummarySubDir", "summary"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleSitchingSubDir", "stitching"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-mergeAlgorithm", mergeAlgorithm))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-channelDyeSpec", channelDyeSpec))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-outputChannelOrder", outputChannelOrder)))
                    );

                    verify(vaa3dStitchAndBlendProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-inputDir", new File(SampleProcessorTestUtils.TEST_WORKING_DIR + "/" + area + "/group").getAbsolutePath()))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-outputFile", new File(SampleProcessorTestUtils.TEST_WORKING_DIR + "/" + area + "/stitch/" + "stitched-" + objective + area + ".v3draw").getAbsolutePath()))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-refchannel", TEST_REFERENCE_CHANNELS)))
                    );

                    verify(mipGenerationProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class)
                    );

                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    fail(exc.toString());
                    return null;
                });

        verify(failure, never()).accept(any());
        verify(successful).accept(any());
    }

    @Test
    public void processWithoutStitching() {
        String area = "brain";
        String objective = "20x";
        String mergeAlgorithm = "FLYLIGHT_ORDERED";
        String channelDyeSpec = "reference=Alexa Fluor 488,Cy2;" +
                "membrane_ha=,ATTO 647,Alexa Fluor 633,Alexa Fluor 647,Cy5;" +
                "membrane_v5=Alexa Fluor 546,Alexa Fluor 555,Alexa Fluor 568,DY-547;" +
                "membrane_flag=Alexa Fluor 594";
        String outputChannelOrder = "membrane_ha,membrane_v5,membrane_flag,reference";
        Long testSampleId = SampleProcessorTestUtils.TEST_SAMPLE_ID;


        ServiceResultHandler<List<SampleAreaResult>> mergeAndGroupSampleTilePairsResultHandler = mock(ServiceResultHandler.class);
        when(mergeAndGroupSampleTilePairsProcessor.getResultHandler()).thenReturn(mergeAndGroupSampleTilePairsResultHandler);
        when(mergeAndGroupSampleTilePairsResultHandler.getServiceDataResult(any(JacsServiceData.class))).then(invocation -> {
            return ImmutableList.of(
                    createSampleAreaResult(
                            testSampleId,
                            objective,
                            area,
                            ImmutableList.of(
                                    createTilePairResult("t1", "rt1")
                            ),
                            ImmutableList.of(
                                    createTilePairResult("tm1", "rtm1")
                            )
                    )
            );
        });

        ServiceResultHandler<StitchAndBlendResult> vaa3dStitchAndBlendResultHandler = mock(ServiceResultHandler.class);
        when(vaa3dStitchAndBlendProcessor.getResultHandler()).thenReturn(vaa3dStitchAndBlendResultHandler);
        when(vaa3dStitchAndBlendResultHandler.getServiceDataResult(any(JacsServiceData.class))).then(invocation -> {
            StitchAndBlendResult stitchAndBlendResult = new StitchAndBlendResult();
            stitchAndBlendResult.setStitchedFile(new File("stitched"));
            stitchAndBlendResult.setStitchedImageInfoFile(new File("stitchInfo"));
            return stitchAndBlendResult;
        });

        ServiceResultHandler<List<File>> mipGenerationResultHandler = mock(ServiceResultHandler.class);
        when(mipGenerationProcessor.getResultHandler()).thenReturn(mipGenerationResultHandler);
        when(mipGenerationResultHandler.getServiceDataResult(any(JacsServiceData.class))).then(invocation -> {
            return ImmutableList.of(new File("mip1"), new File("mip2"));
        });

        JacsServiceData testServiceData = createTestServiceData(1L,
                testSampleId,
                area,
                objective,
                mergeAlgorithm,
                channelDyeSpec,
                outputChannelOrder,
                true
        );

        ServiceComputation<JacsServiceResult<SampleResult>> stitchProcessing = sampleStitchProcessor.process(testServiceData);
        Consumer successful = mock(Consumer.class);
        Consumer failure = mock(Consumer.class);
        stitchProcessing
                .thenApply(r -> {
                    successful.accept(r);

                    verify(mergeAndGroupSampleTilePairsProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", testSampleId))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-area", area))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleResultsId", TEST_RESULTS_ID))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataRootDir", SampleProcessorTestUtils.TEST_WORKING_DIR))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleLsmsSubDir", "lsms"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleSummarySubDir", "summary"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleSitchingSubDir", "stitching"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-mergeAlgorithm", mergeAlgorithm))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-channelDyeSpec", channelDyeSpec))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-outputChannelOrder", outputChannelOrder)))
                    );

                    verify(vaa3dStitchAndBlendProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class)
                    );

                    verify(mipGenerationProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-inputFile", "rtm1"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-outputDir", new File(SampleProcessorTestUtils.TEST_WORKING_DIR + "/" + area + "/mips").getAbsolutePath()))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-signalChannels", TEST_SIGNAL_CHANNELS))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-referenceChannel", TEST_REFERENCE_CHANNELS))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-imgFormat", "png")))
                    );

                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    fail(exc.toString());
                    return null;
                });

        verify(failure, never()).accept(any());
        verify(successful).accept(any());
    }

    @Test
    public void processWithoutStitchingAndWithNoMips() {
        String area = "brain";
        String objective = "20x";
        String mergeAlgorithm = "FLYLIGHT_ORDERED";
        String channelDyeSpec = "reference=Alexa Fluor 488,Cy2;" +
                "membrane_ha=,ATTO 647,Alexa Fluor 633,Alexa Fluor 647,Cy5;" +
                "membrane_v5=Alexa Fluor 546,Alexa Fluor 555,Alexa Fluor 568,DY-547;" +
                "membrane_flag=Alexa Fluor 594";
        String outputChannelOrder = "membrane_ha,membrane_v5,membrane_flag,reference";
        Long testSampleId = SampleProcessorTestUtils.TEST_SAMPLE_ID;


        ServiceResultHandler<List<SampleAreaResult>> mergeAndGroupSampleTilePairsResultHandler = mock(ServiceResultHandler.class);
        when(mergeAndGroupSampleTilePairsProcessor.getResultHandler()).thenReturn(mergeAndGroupSampleTilePairsResultHandler);
        when(mergeAndGroupSampleTilePairsResultHandler.getServiceDataResult(any(JacsServiceData.class))).then(invocation -> {
            return ImmutableList.of(
                    createSampleAreaResult(
                            testSampleId,
                            objective,
                            area,
                            ImmutableList.of(
                                    createTilePairResult("t1", "rt1")
                            ),
                            ImmutableList.of(
                                    createTilePairResult("tm1", "rtm1")
                            )
                    )
            );
        });

        ServiceResultHandler<StitchAndBlendResult> vaa3dStitchAndBlendResultHandler = mock(ServiceResultHandler.class);
        when(vaa3dStitchAndBlendProcessor.getResultHandler()).thenReturn(vaa3dStitchAndBlendResultHandler);
        when(vaa3dStitchAndBlendResultHandler.getServiceDataResult(any(JacsServiceData.class))).then(invocation -> {
            StitchAndBlendResult stitchAndBlendResult = new StitchAndBlendResult();
            stitchAndBlendResult.setStitchedFile(new File("stitched"));
            stitchAndBlendResult.setStitchedImageInfoFile(new File("stitchInfo"));
            return stitchAndBlendResult;
        });

        ServiceResultHandler<List<File>> mipGenerationResultHandler = mock(ServiceResultHandler.class);
        when(mipGenerationProcessor.getResultHandler()).thenReturn(mipGenerationResultHandler);
        when(mipGenerationResultHandler.getServiceDataResult(any(JacsServiceData.class))).then(invocation -> {
            return ImmutableList.of(new File("mip1"), new File("mip2"));
        });

        JacsServiceData testServiceData = createTestServiceData(1L,
                testSampleId,
                area,
                objective,
                mergeAlgorithm,
                channelDyeSpec,
                outputChannelOrder,
                false
        );

        ServiceComputation<JacsServiceResult<SampleResult>> stitchProcessing = sampleStitchProcessor.process(testServiceData);
        Consumer successful = mock(Consumer.class);
        Consumer failure = mock(Consumer.class);
        stitchProcessing
                .thenApply(r -> {
                    successful.accept(r);

                    verify(mergeAndGroupSampleTilePairsProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", testSampleId))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-area", area))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleResultsId", TEST_RESULTS_ID))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataRootDir", SampleProcessorTestUtils.TEST_WORKING_DIR))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleLsmsSubDir", "lsms"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleSummarySubDir", "summary"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleSitchingSubDir", "stitching"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-mergeAlgorithm", mergeAlgorithm))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-channelDyeSpec", channelDyeSpec))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-outputChannelOrder", outputChannelOrder)))
                    );

                    verify(vaa3dStitchAndBlendProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class)
                    );

                    verify(mipGenerationProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class)
                    );

                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    fail(exc.toString());
                    return null;
                });

        verify(failure, never()).accept(any());
        verify(successful).accept(any());
    }

    private SampleAreaResult createSampleAreaResult(Number sampleId, String objective, String areaName, List<MergeTilePairResult> mergedTiles, List<MergeTilePairResult> groupedTiles) {
        SampleAreaResult ar = new SampleAreaResult();
        ar.setSampleId(sampleId);
        ar.setResultDir(new File(SampleProcessorTestUtils.TEST_WORKING_DIR + "/" + areaName).getAbsolutePath());
        ar.setMergeRelativeSubDir("merge");
        ar.setGroupRelativeSubDir("group");
        ar.setConsensusChannelComponents(createChannelComponents());
        ar.setObjective(objective);
        ar.setAnatomicalArea(areaName);
        ar.setMergeResults(mergedTiles);
        ar.setGroupResults(groupedTiles);
        return ar;
    }

    private MergeTilePairResult createTilePairResult(String tn, String mergeResultFn) {
        MergeTilePairResult tpr = new MergeTilePairResult();
        tpr.setTileName(tn);
        tpr.setImageSize("10x20x30");
        tpr.setOpticalResolution("0.45x0.45");
        tpr.setMergeResultFile(mergeResultFn);
        return tpr;
    }

    private ChannelComponents createChannelComponents() {
        ChannelComponents chComp = new ChannelComponents();
        chComp.channelSpec = "ssr";
        chComp.signalChannelsPos = TEST_SIGNAL_CHANNELS;
        chComp.referenceChannelsPos = "2";
        chComp.referenceChannelNumbers = TEST_REFERENCE_CHANNELS;
        return chComp;
    }

    private JacsServiceData createTestServiceData(Long serviceId,
                                                  Long sampleId, String area, String objective,
                                                  String mergeAlgorithm,
                                                  String channelDyeSpec, String outputChannelOrder,
                                                  boolean generateMips) {
        JacsServiceDataBuilder testServiceDataBuilder = new JacsServiceDataBuilder(null)
                .addArg("-sampleId", String.valueOf(sampleId))
                .addArg("-area", area)
                .addArg("-objective", objective)
                .addArg("-sampleResultsId", TEST_RESULTS_ID)
                .addArg("-sampleDataRootDir", SampleProcessorTestUtils.TEST_WORKING_DIR)
                .addArg("-sampleLsmsSubDir", "lsms")
                .addArg("-sampleSummarySubDir", "summary")
                .addArg("-sampleSitchingSubDir", "stitching")
                .setWorkspace(SampleProcessorTestUtils.TEST_WORKING_DIR);

        if (StringUtils.isNotBlank(mergeAlgorithm))
            testServiceDataBuilder.addArg("-mergeAlgorithm", mergeAlgorithm);

        if (StringUtils.isNotBlank(channelDyeSpec))
            testServiceDataBuilder.addArg("-channelDyeSpec", channelDyeSpec);

        if (StringUtils.isNotBlank(outputChannelOrder))
            testServiceDataBuilder.addArg("-outputChannelOrder", outputChannelOrder);

        if (generateMips)
            testServiceDataBuilder.addArg("-generateMips");
        JacsServiceData testServiceData = testServiceDataBuilder.build();
        testServiceData.setId(serviceId);
        return testServiceData;
    }

}
