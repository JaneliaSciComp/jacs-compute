package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.it.jacs.model.domain.sample.SamplePipelineRun;
import org.janelia.it.jacs.model.domain.sample.SamplePostProcessingResult;
import org.janelia.jacs2.asyncservice.alignservices.AlignmentServiceBuilderFactory;
import org.janelia.jacs2.asyncservice.alignservices.AlignmentProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationTestUtils;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgMatcher;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.imageservices.BasicMIPsAndMoviesProcessor;
import org.janelia.jacs2.asyncservice.imageservices.EnhancedMIPsAndMoviesProcessor;
import org.janelia.jacs2.asyncservice.neuronservices.NeuronSeparationFiles;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceDataBuilder;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FlylightSampleProcessorTest {
    private static final String DEFAULT_MIP_MAPS_OPTIONS = "mips:movies:legends:bcomp";
    private static final Long TEST_RUN_ID = 20L;
    private static final Long TEST_RESULT_ID = 30L;
    private static final String TEST_AREA_FILE = "anAreaFile.txt";

    private ServiceComputationFactory computationFactory;
    private SampleDataService sampleDataService;
    private InitializeSamplePipelineResultsProcessor initializeSamplePipelineResultsProcessor;
    private GetSampleImageFilesProcessor getSampleImageFilesProcessor;
    private SampleLSMSummaryProcessor sampleLSMSummaryProcessor;
    private SampleStitchProcessor sampleStitchProcessor;
    private UpdateSamplePipelineResultsProcessor updateSamplePipelineResultsProcessor;
    private BasicMIPsAndMoviesProcessor basicMIPsAndMoviesProcessor;
    private EnhancedMIPsAndMoviesProcessor enhancedMIPsAndMoviesProcessor;
    private UpdateSamplePostProcessingPipelineResultsProcessor updateSamplePostProcessingPipelineResultsProcessor;
    private SampleNeuronSeparationProcessor sampleNeuronSeparationProcessor;
    private FlylightSampleProcessor flylightSampleProcessor;
    private AlignmentProcessor alignmentProcessor;
    private UpdateAlignmentResultsProcessor updateAlignmentResultsProcessor;
    private SampleNeuronWarpingProcessor sampleNeuronWarpingProcessor;

    @Before
    public void setUp() {
        Logger logger = mock(Logger.class);

        computationFactory = ComputationTestUtils.createTestServiceComputationFactory(logger);

        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        sampleDataService = mock(SampleDataService.class);
        initializeSamplePipelineResultsProcessor = mock(InitializeSamplePipelineResultsProcessor.class);
        getSampleImageFilesProcessor = mock(GetSampleImageFilesProcessor.class);
        sampleLSMSummaryProcessor = mock(SampleLSMSummaryProcessor.class);
        sampleStitchProcessor = mock(SampleStitchProcessor.class);
        updateSamplePipelineResultsProcessor = mock(UpdateSamplePipelineResultsProcessor.class);
        BasicMIPsAndMoviesProcessor basicMIPsAndMoviesProcessor = mock(BasicMIPsAndMoviesProcessor.class);
        EnhancedMIPsAndMoviesProcessor enhancedMIPsAndMoviesProcessor = mock(EnhancedMIPsAndMoviesProcessor.class);
        updateSamplePostProcessingPipelineResultsProcessor = mock(UpdateSamplePostProcessingPipelineResultsProcessor.class);
        sampleNeuronSeparationProcessor = mock(SampleNeuronSeparationProcessor.class);
        AlignmentServiceBuilderFactory alignmentServiceBuilderFactory = mock(AlignmentServiceBuilderFactory.class);
        alignmentProcessor = mock(AlignmentProcessor.class);
        updateAlignmentResultsProcessor = mock(UpdateAlignmentResultsProcessor.class);
        sampleNeuronWarpingProcessor = mock(SampleNeuronWarpingProcessor.class);

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

        when(initializeSamplePipelineResultsProcessor.getMetadata()).thenCallRealMethod();
        when(initializeSamplePipelineResultsProcessor.createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        )).thenCallRealMethod();

        when(getSampleImageFilesProcessor.getMetadata()).thenCallRealMethod();
        when(getSampleImageFilesProcessor.createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        )).thenCallRealMethod();

        when(sampleLSMSummaryProcessor.getMetadata()).thenCallRealMethod();
        when(sampleLSMSummaryProcessor.createServiceData(any(ServiceExecutionContext.class),
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
        )).thenCallRealMethod();

        when(sampleStitchProcessor.getMetadata()).thenCallRealMethod();
        when(sampleStitchProcessor.createServiceData(any(ServiceExecutionContext.class),
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
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class)
                )
        ).thenCallRealMethod();

        when(updateSamplePipelineResultsProcessor.getMetadata()).thenCallRealMethod();
        when(updateSamplePipelineResultsProcessor.createServiceData(any(ServiceExecutionContext.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class)
                )
        ).thenCallRealMethod();

        flylightSampleProcessor = new FlylightSampleProcessor(computationFactory,
                jacsServiceDataPersistence,
                SampleProcessorTestUtils.TEST_WORKING_DIR,
                sampleDataService,
                initializeSamplePipelineResultsProcessor,
                getSampleImageFilesProcessor,
                sampleLSMSummaryProcessor,
                sampleStitchProcessor,
                updateSamplePipelineResultsProcessor,
                basicMIPsAndMoviesProcessor,
                enhancedMIPsAndMoviesProcessor,
                updateSamplePostProcessingPipelineResultsProcessor,
                sampleNeuronSeparationProcessor,
                alignmentServiceBuilderFactory,
                alignmentProcessor,
                updateAlignmentResultsProcessor,
                sampleNeuronWarpingProcessor,
                logger);
    }

    @Test
    public void withSummaryNoSeparationAndNoAlignment() {
        String area = "area";
        String objective = "objective";
        String mergeAlgorithm = "FLYLIGHT_ORDERED";
        String channelDyeSpec = "reference=Alexa Fluor 488,Cy2;" +
                "membrane_ha=,ATTO 647,Alexa Fluor 633,Alexa Fluor 647,Cy5;" +
                "membrane_v5=Alexa Fluor 546,Alexa Fluor 555,Alexa Fluor 568,DY-547;" +
                "membrane_flag=Alexa Fluor 594";
        String outputChannelOrder = "membrane_ha,membrane_v5,membrane_flag,reference";

        Map<String, String> testData = ImmutableMap.of(
                "1234", "1234",
                "1234567891234567", "234/567/1234567891234567"
        );

        ServiceResultHandler<List<SamplePipelineRun>> initializeSamplePipelineResultsResultHandler = mock(ServiceResultHandler.class);
        when(initializeSamplePipelineResultsProcessor.getResultHandler()).thenReturn(initializeSamplePipelineResultsResultHandler);
        when(initializeSamplePipelineResultsResultHandler.getServiceDataResult(any(JacsServiceData.class))).thenReturn(ImmutableList.of());

        ServiceResultHandler<List<SampleImageFile>> getSampleImageFilesResultHandler = mock(ServiceResultHandler.class);
        when(getSampleImageFilesProcessor.getResultHandler()).thenReturn(getSampleImageFilesResultHandler);
        when(getSampleImageFilesResultHandler.getServiceDataResult(any(JacsServiceData.class))).thenReturn(ImmutableList.of());

        ServiceResultHandler<List<LSMSummary>> sampleLSMSummaryResultHandler = mock(ServiceResultHandler.class);
        when(sampleLSMSummaryProcessor.getResultHandler()).thenReturn(sampleLSMSummaryResultHandler);
        when(sampleLSMSummaryResultHandler.getServiceDataResult(any(JacsServiceData.class))).thenReturn(ImmutableList.of());

        ServiceResultHandler<SampleResult> sampleStitchResultHandler = mock(ServiceResultHandler.class);
        when(sampleStitchProcessor.getResultHandler()).thenReturn(sampleStitchResultHandler);
        when(sampleStitchResultHandler.getServiceDataResult(any(JacsServiceData.class))).thenReturn(new SampleResult());

        ServiceResultHandler<List<SampleProcessorResult>> updateSamplePipelineResultsResultHandler = mock(ServiceResultHandler.class);
        when(updateSamplePipelineResultsProcessor.getResultHandler()).thenReturn(updateSamplePipelineResultsResultHandler);
        when(updateSamplePipelineResultsResultHandler.getServiceDataResult(any(JacsServiceData.class))).thenReturn(ImmutableList.of(new SampleProcessorResult()));

        int dataIndex = 0;
        for (Map.Entry<String, String> testEntry : testData.entrySet()) {
            int testDataIndex = dataIndex;
            Long testServiceId = Long.valueOf(testEntry.getKey());
            String testSampleDir = SampleProcessorTestUtils.TEST_WORKING_DIR;
            String testLsmSubDir =  testEntry.getValue();
            JacsServiceData testServiceData = createTestServiceData(1L,
                    SampleProcessorTestUtils.TEST_SAMPLE_ID,
                    area,
                    objective,
                    mergeAlgorithm,
                    channelDyeSpec,
                    outputChannelOrder,
                    false, // do not skipSummary
                    true, // montageMipMaps
                    true, // distortionCorrection
                    true, // persistResults (mipmaps)
                    false, // no neuron separation
                    null   // no alignment
            );
            testServiceData.setId(testServiceId);

            ServiceComputation<JacsServiceResult<List<SampleProcessorResult>>> flylightProcessing = flylightSampleProcessor.process(testServiceData);
            Consumer successful = mock(Consumer.class);
            Consumer failure = mock(Consumer.class);
            flylightProcessing
                    .thenApply(r -> {
                        successful.accept(r);

                        verify(getSampleImageFilesProcessor).createServiceData(any(ServiceExecutionContext.class),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", SampleProcessorTestUtils.TEST_SAMPLE_ID.toString()))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-area", area))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataRootDir", testSampleDir))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleLsmsSubDir", "Temp" + "/" + testLsmSubDir)))
                        );

                        verify(sampleLSMSummaryProcessor).createServiceData(any(ServiceExecutionContext.class),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", SampleProcessorTestUtils.TEST_SAMPLE_ID.toString()))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-area", area))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleResultsId", testServiceId))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataRootDir", testSampleDir))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleLsmsSubDir", "Temp" + "/" + testLsmSubDir))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleSummarySubDir", "Summary" + "/" + testLsmSubDir))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-channelDyeSpec", channelDyeSpec))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-basicMipMapsOptions", DEFAULT_MIP_MAPS_OPTIONS))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-montageMipMaps", true)))
                        );

                        verify(sampleStitchProcessor).createServiceData(any(ServiceExecutionContext.class),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", SampleProcessorTestUtils.TEST_SAMPLE_ID.toString()))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-area", area))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleResultsId", testServiceId))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataRootDir", testSampleDir))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleLsmsSubDir", "Temp" + "/" + testLsmSubDir))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleSummarySubDir", "Summary" + "/" + testLsmSubDir))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleSitchingSubDir", "Sample" + "/" + testLsmSubDir))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-mergeAlgorithm", mergeAlgorithm))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-channelDyeSpec", channelDyeSpec))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-outputChannelOrder", outputChannelOrder))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-distortionCorrection", true))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-generateMips", true)))
                        );

                        verify(updateSamplePipelineResultsProcessor).createServiceData(any(ServiceExecutionContext.class),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleResultsId", testServiceId))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleProcessingId", SampleProcessorTestUtils.TEST_SERVICE_ID.toString())))
                        );

                        verify(sampleNeuronSeparationProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                                any(ServiceArg.class),
                                any(ServiceArg.class),
                                any(ServiceArg.class),
                                any(ServiceArg.class),
                                any(ServiceArg.class),
                                any(ServiceArg.class),
                                any(ServiceArg.class),
                                any(ServiceArg.class),
                                any(ServiceArg.class)
                        );

                        verify(alignmentProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                                any(ServiceArg.class),
                                any(ServiceArg.class),
                                any(ServiceArg.class),
                                any(ServiceArg.class),
                                any(ServiceArg.class),
                                any(ServiceArg.class),
                                any(ServiceArg.class),
                                any(ServiceArg.class)
                        );

                        verify(updateAlignmentResultsProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                                any(ServiceArg.class),
                                any(ServiceArg.class),
                                any(ServiceArg.class),
                                any(ServiceArg.class),
                                any(ServiceArg.class)
                        );

                        verify(sampleNeuronWarpingProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
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
                        );

                        return r;
                    })
                    .exceptionally(exc -> {
                        failure.accept(exc);
                        fail(exc.toString());
                        return null;
                    });
            dataIndex++;
        }
    }

    @Test
    public void noSummaryWithSampleProcessingSeparationAndNoAlignment() {
        String area = "area";
        String objective = "objective";
        String mergeAlgorithm = "FLYLIGHT_ORDERED";
        String channelDyeSpec = "reference=Alexa Fluor 488,Cy2;" +
                "membrane_ha=,ATTO 647,Alexa Fluor 633,Alexa Fluor 647,Cy5;" +
                "membrane_v5=Alexa Fluor 546,Alexa Fluor 555,Alexa Fluor 568,DY-547;" +
                "membrane_flag=Alexa Fluor 594";
        String outputChannelOrder = "membrane_ha,membrane_v5,membrane_flag,reference";

        when(sampleDataService.getSampleById(argThat(argument -> {
            return argument == null || "testOwner".equals(argument);
        }), eq(SampleProcessorTestUtils.TEST_SAMPLE_ID)))
                .thenReturn(SampleProcessorTestUtils.createTestSample(SampleProcessorTestUtils.TEST_SAMPLE_ID, objective, area));

        ServiceResultHandler<List<SamplePipelineRun>> initializeSamplePipelineResultsResultHandler = mock(ServiceResultHandler.class);
        when(initializeSamplePipelineResultsProcessor.getResultHandler()).thenReturn(initializeSamplePipelineResultsResultHandler);
        when(initializeSamplePipelineResultsResultHandler.getServiceDataResult(any(JacsServiceData.class))).thenReturn(ImmutableList.of());

        ServiceResultHandler<List<SampleImageFile>> getSampleImageFilesResultHandler = mock(ServiceResultHandler.class);
        when(getSampleImageFilesProcessor.getResultHandler()).thenReturn(getSampleImageFilesResultHandler);
        when(getSampleImageFilesResultHandler.getServiceDataResult(any(JacsServiceData.class))).then(invocation -> {
            return new JacsServiceResult<>(invocation.getArgument(0), ImmutableList.of());
        });

        ServiceResultHandler<SampleResult> sampleStitchResultHandler = mock(ServiceResultHandler.class);
        when(sampleStitchProcessor.getResultHandler()).thenReturn(sampleStitchResultHandler);
        when(sampleStitchResultHandler.getServiceDataResult(any(JacsServiceData.class))).thenReturn(new SampleResult());

        ServiceResultHandler<List<SampleProcessorResult>> updateSamplePipelineResultsResultHandler = mock(ServiceResultHandler.class);
        when(updateSamplePipelineResultsProcessor.getResultHandler()).thenReturn(updateSamplePipelineResultsResultHandler);
        when(updateSamplePipelineResultsResultHandler.getServiceDataResult(any(JacsServiceData.class)))
                .thenReturn(ImmutableList.of(createSampleProcessorResult(SampleProcessorTestUtils.TEST_SAMPLE_ID, objective, area)));

        ServiceResultHandler<NeuronSeparationFiles> sampleNeuronSeparationResultHandler = mock(ServiceResultHandler.class);
        when(sampleNeuronSeparationProcessor.getResultHandler()).thenReturn(sampleNeuronSeparationResultHandler);
        when(sampleNeuronSeparationResultHandler.getServiceDataResult(any(JacsServiceData.class))).thenReturn(new NeuronSeparationFiles());

        String testServiceId = "1234567891234567";
        String testLsmSubDir = "234/567/1234567891234567";
        String testSampleDir = SampleProcessorTestUtils.TEST_WORKING_DIR;
        JacsServiceData testServiceData = createTestServiceData(1L,
                SampleProcessorTestUtils.TEST_SAMPLE_ID,
                area,
                objective,
                mergeAlgorithm,
                channelDyeSpec,
                outputChannelOrder,
                true, // skipSummary
                true, // montageMipMaps
                true, // distortionCorrection
                true, // persistResults (mipmaps)
                true, // run post sample processing neuron separation
                null   // no alignment
        );
        testServiceData.setId(Long.valueOf(testServiceId));

        ServiceComputation<JacsServiceResult<List<SampleProcessorResult>>> flylightProcessing = flylightSampleProcessor.process(testServiceData);
        Consumer successful = mock(Consumer.class);
        Consumer failure = mock(Consumer.class);
        flylightProcessing
                .thenApply(r -> {
                    successful.accept(r);

                    verify(getSampleImageFilesProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", SampleProcessorTestUtils.TEST_SAMPLE_ID.toString()))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-area", area))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataRootDir", testSampleDir))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleLsmsSubDir", "Temp" + "/" + testLsmSubDir)))
                    );

                    verify(sampleLSMSummaryProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
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
                    );

                    verify(sampleStitchProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", SampleProcessorTestUtils.TEST_SAMPLE_ID.toString()))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-area", area))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleResultsId", testServiceId))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataRootDir", testSampleDir))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleLsmsSubDir", "Temp" + "/" + testLsmSubDir))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleSummarySubDir", "Summary" + "/" + testLsmSubDir))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleSitchingSubDir", "Sample" + "/" + testLsmSubDir))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-mergeAlgorithm", mergeAlgorithm))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-channelDyeSpec", channelDyeSpec))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-outputChannelOrder", outputChannelOrder))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-distortionCorrection", true))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-generateMips", true)))
                    );

                    verify(updateSamplePipelineResultsProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleResultsId", testServiceId))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleProcessingId", SampleProcessorTestUtils.TEST_SERVICE_ID.toString())))
                    );

                    verify(sampleNeuronSeparationProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", SampleProcessorTestUtils.TEST_SAMPLE_ID))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-runId", TEST_RUN_ID))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-resultId", TEST_RESULT_ID))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-inputFile", TEST_AREA_FILE))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-outputDir", testSampleDir + "/" + "Separation" + "/" + TEST_RESULT_ID + "/" + area))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-signalChannels", "0 1"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-referenceChannel", "3"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-previousResultFile", "")))
                    );

                    verify(alignmentProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class)
                    );

                    verify(updateAlignmentResultsProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class)
                    );

                    verify(sampleNeuronWarpingProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
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
                    );

                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    fail(exc.toString());
                    return null;
                });
    }

    private JacsServiceData createTestServiceData(Long serviceId,
                                                  Long sampleId, String area, String objective,
                                                  String mergeAlgorithm,
                                                  String channelDyeSpec, String outputChannelOrder,
                                                  boolean skipSummary,
                                                  boolean montageMipMaps,
                                                  boolean useDistortionCorrection,
                                                  boolean persistResults,
                                                  boolean runNeuronSeparationAfterSampleProcessing,
                                                  String alignmentAlgorithm) {
        JacsServiceDataBuilder testServiceDataBuilder = new JacsServiceDataBuilder(null)
                .setOwner("testOwner")
                .addArg("-sampleId", String.valueOf(sampleId))
                .addArg("-area", area)
                .addArg("-objective", objective)
                .addArg("-sampleDataRootDir", SampleProcessorTestUtils.TEST_WORKING_DIR)
                .setWorkspace(SampleProcessorTestUtils.TEST_WORKING_DIR);

        if (StringUtils.isNotBlank(mergeAlgorithm))
            testServiceDataBuilder.addArg("-mergeAlgorithm", mergeAlgorithm);

        if (StringUtils.isNotBlank(channelDyeSpec))
            testServiceDataBuilder.addArg("-channelDyeSpec", channelDyeSpec);

        if (StringUtils.isNotBlank(outputChannelOrder))
            testServiceDataBuilder.addArg("-outputChannelOrder", outputChannelOrder);

        if (skipSummary)
            testServiceDataBuilder.addArg("-skipSummary");

        if (montageMipMaps)
            testServiceDataBuilder.addArg("-montageMipMaps");

        if (useDistortionCorrection)
            testServiceDataBuilder.addArg("-distortionCorrection");

        if (persistResults)
            testServiceDataBuilder.addArg("-persistResults");

        if (StringUtils.isNotBlank(alignmentAlgorithm))
            testServiceDataBuilder.addArg("-alignmentAlgorithm", alignmentAlgorithm);

        if (runNeuronSeparationAfterSampleProcessing)
            testServiceDataBuilder.addArg("-runNeuronSeparationAfterSampleProcessing");

        JacsServiceData testServiceData = testServiceDataBuilder.build();
        testServiceData.setId(serviceId);
        return testServiceData;
    }

    private SampleProcessorResult createSampleProcessorResult(Number sampleId, String objective, String area) {
        SampleProcessorResult sampleProcessorResult = new SampleProcessorResult();
        sampleProcessorResult.setSampleId(sampleId);
        sampleProcessorResult.setObjective(objective);
        sampleProcessorResult.setArea(area);
        sampleProcessorResult.setRunId(TEST_RUN_ID);
        sampleProcessorResult.setResultId(TEST_RESULT_ID);
        sampleProcessorResult.setAreaFile(TEST_AREA_FILE);
        sampleProcessorResult.setSignalChannels("0 1");
        sampleProcessorResult.setReferenceChannel("3");
        return sampleProcessorResult;
    }

}
