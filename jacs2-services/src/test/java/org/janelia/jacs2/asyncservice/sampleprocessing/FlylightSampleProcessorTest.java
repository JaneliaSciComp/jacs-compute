package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.janelia.model.jacs2.domain.sample.Sample;
import org.janelia.model.jacs2.domain.sample.SamplePipelineRun;
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
import org.janelia.jacs2.asyncservice.imageservices.BasicMIPsAndMoviesProcessor;
import org.janelia.jacs2.asyncservice.imageservices.EnhancedMIPsAndMoviesProcessor;
import org.janelia.jacs2.asyncservice.imageservices.MIPsAndMoviesResult;
import org.janelia.jacs2.asyncservice.imageservices.tools.ChannelComponents;
import org.janelia.jacs2.asyncservice.neuronservices.NeuronSeparationFiles;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.model.security.util.SubjectUtils;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.janelia.model.service.JacsServiceState;
import org.janelia.model.service.ServiceMetaData;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FlylightSampleProcessorTest {
    private static final String DEFAULT_MIP_MAPS_OPTIONS = "mips:movies:legends:bcomp";
    private static final Long TEST_RUN_ID = 20L;
    private static final Long TEST_RESULT_ID = 30L;
    private static final String TEST_AREA_FILE = "anAreaFile.txt";
    private static final String TEST_MIPS_DIR = "mipsDir";

    private SampleDataService sampleDataService;
    private InitializeSamplePipelineResultsProcessor initializeSamplePipelineResultsProcessor;
    private GetSampleImageFilesProcessor getSampleImageFilesProcessor;
    private SampleLSMSummaryProcessor sampleLSMSummaryProcessor;
    private UpdateSampleSummaryResultsProcessor updateSampleSummaryResultsProcessor;
    private SampleStitchProcessor sampleStitchProcessor;
    private UpdateSampleProcessingResultsProcessor updateSampleProcessingResultsProcessor;
    private BasicMIPsAndMoviesProcessor basicMIPsAndMoviesProcessor;
    private EnhancedMIPsAndMoviesProcessor enhancedMIPsAndMoviesProcessor;
    private UpdateSamplePostProcessingResultsProcessor updateSamplePostProcessingResultsProcessor;
    private SampleNeuronSeparationProcessor sampleNeuronSeparationProcessor;
    private FlylightSampleProcessor flylightSampleProcessor;
    private AlignmentProcessor alignmentProcessor;
    private UpdateAlignmentResultsProcessor updateAlignmentResultsProcessor;
    private SampleNeuronWarpingProcessor sampleNeuronWarpingProcessor;
    private CleanSampleImageFilesProcessor cleanSampleImageFilesProcessor;
    private SampleResultsCompressionProcessor sampleResultsCompressionProcessor;

    @Before
    public void setUp() {
        Logger logger = mock(Logger.class);

        ServiceComputationFactory computationFactory = ComputationTestUtils.createTestServiceComputationFactory(logger);

        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        sampleDataService = mock(SampleDataService.class);
        initializeSamplePipelineResultsProcessor = mock(InitializeSamplePipelineResultsProcessor.class);
        getSampleImageFilesProcessor = mock(GetSampleImageFilesProcessor.class);
        sampleLSMSummaryProcessor = mock(SampleLSMSummaryProcessor.class);
        updateSampleSummaryResultsProcessor = mock(UpdateSampleSummaryResultsProcessor.class);
        sampleStitchProcessor = mock(SampleStitchProcessor.class);
        updateSampleProcessingResultsProcessor = mock(UpdateSampleProcessingResultsProcessor.class);
        basicMIPsAndMoviesProcessor = mock(BasicMIPsAndMoviesProcessor.class);
        enhancedMIPsAndMoviesProcessor = mock(EnhancedMIPsAndMoviesProcessor.class);
        updateSamplePostProcessingResultsProcessor = mock(UpdateSamplePostProcessingResultsProcessor.class);
        sampleNeuronSeparationProcessor = mock(SampleNeuronSeparationProcessor.class);
        AlignmentServiceBuilderFactory alignmentServiceBuilderFactory = mock(AlignmentServiceBuilderFactory.class);
        alignmentProcessor = mock(AlignmentProcessor.class);
        updateAlignmentResultsProcessor = mock(UpdateAlignmentResultsProcessor.class);
        sampleNeuronWarpingProcessor = mock(SampleNeuronWarpingProcessor.class);
        cleanSampleImageFilesProcessor = mock(CleanSampleImageFilesProcessor.class);
        sampleResultsCompressionProcessor = mock(SampleResultsCompressionProcessor.class);

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
                        any(ServiceArg.class)
                )
        ).thenCallRealMethod();

        when(updateSampleProcessingResultsProcessor.getMetadata()).thenCallRealMethod();
        when(updateSampleProcessingResultsProcessor.createServiceData(any(ServiceExecutionContext.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class)
                )
        ).thenCallRealMethod();

        when(basicMIPsAndMoviesProcessor.getMetadata()).thenCallRealMethod();
        when(basicMIPsAndMoviesProcessor.createServiceData(any(ServiceExecutionContext.class),
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

        when(enhancedMIPsAndMoviesProcessor.getMetadata()).thenCallRealMethod();
        when(enhancedMIPsAndMoviesProcessor.createServiceData(any(ServiceExecutionContext.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class)
                )
        ).thenCallRealMethod();

        when(sampleNeuronSeparationProcessor.getMetadata()).thenCallRealMethod();
        when(sampleNeuronSeparationProcessor.createServiceData(any(ServiceExecutionContext.class),
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

        when(cleanSampleImageFilesProcessor.getMetadata()).thenCallRealMethod();
        when(cleanSampleImageFilesProcessor.createServiceData(any(ServiceExecutionContext.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class)
                )
        ).thenCallRealMethod();
        when(cleanSampleImageFilesProcessor.getResultHandler()).thenCallRealMethod();

        flylightSampleProcessor = new FlylightSampleProcessor(computationFactory,
                jacsServiceDataPersistence,
                SampleProcessorTestUtils.TEST_WORKING_DIR,
                sampleDataService,
                initializeSamplePipelineResultsProcessor,
                getSampleImageFilesProcessor,
                sampleLSMSummaryProcessor,
                updateSampleSummaryResultsProcessor,
                sampleStitchProcessor,
                updateSampleProcessingResultsProcessor,
                basicMIPsAndMoviesProcessor,
                enhancedMIPsAndMoviesProcessor,
                updateSamplePostProcessingResultsProcessor,
                sampleNeuronSeparationProcessor,
                alignmentServiceBuilderFactory,
                alignmentProcessor,
                updateAlignmentResultsProcessor,
                sampleNeuronWarpingProcessor,
                cleanSampleImageFilesProcessor,
                sampleResultsCompressionProcessor,
                logger);
    }

    @Test
    public void serviceMetaData() {
        ServiceMetaData serviceMetaData = flylightSampleProcessor.getMetadata();
        assertThat(serviceMetaData.getServiceName(), equalTo("flylightSample"));
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
        when(updateSampleProcessingResultsProcessor.getResultHandler()).thenReturn(updateSamplePipelineResultsResultHandler);
        when(updateSamplePipelineResultsResultHandler.getServiceDataResult(any(JacsServiceData.class))).thenReturn(ImmutableList.of(new SampleProcessorResult()));

        for (Map.Entry<String, String> testEntry : testData.entrySet()) {
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
                    true, // persistResults (mipmaps)
                    false, // no neuron separation
                    null,  // no alignment algorithm
                    null   // no alignment result
            );
            testServiceData.setId(testServiceId);

            ServiceComputation<JacsServiceResult<List<SampleProcessorResult>>> flylightProcessing = flylightSampleProcessor.process(testServiceData);
            Consumer successful = mock(Consumer.class);
            Consumer failure = mock(Consumer.class);
            flylightProcessing
                    .thenApply(r -> {
                        successful.accept(r);

                        String expectedSampleRootDir = testSampleDir + "/" + SubjectUtils.getSubjectName(testServiceData.getOwnerKey());

                        verify(getSampleImageFilesProcessor).createServiceData(any(ServiceExecutionContext.class),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", SampleProcessorTestUtils.TEST_SAMPLE_ID.toString()))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-area", area))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataRootDir", expectedSampleRootDir))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleLsmsSubDir", "Temp" + "/" + testLsmSubDir)))
                        );

                        verify(sampleLSMSummaryProcessor).createServiceData(any(ServiceExecutionContext.class),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", SampleProcessorTestUtils.TEST_SAMPLE_ID.toString()))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-area", area))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleResultsId", testServiceId))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataRootDir", expectedSampleRootDir))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleLsmsSubDir", "Temp" + "/" + testLsmSubDir))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleSummarySubDir", "Summary" + "/" + testLsmSubDir))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-channelDyeSpec", channelDyeSpec))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-basicMipMapsOptions", DEFAULT_MIP_MAPS_OPTIONS))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-montageMipMaps", true)))
                        );

                        verify(updateSampleSummaryResultsProcessor).createServiceData(any(ServiceExecutionContext.class),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleResultsId", testServiceId))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleSummaryId", SampleProcessorTestUtils.TEST_SERVICE_ID.toString())))
                        );

                        verify(sampleStitchProcessor).createServiceData(any(ServiceExecutionContext.class),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", SampleProcessorTestUtils.TEST_SAMPLE_ID.toString()))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-area", area))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleResultsId", testServiceId))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataRootDir", expectedSampleRootDir))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleLsmsSubDir", "Temp" + "/" + testLsmSubDir))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleSummarySubDir", "Summary" + "/" + testLsmSubDir))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-sampleSitchingSubDir", "Sample" + "/" + testLsmSubDir))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-mergeAlgorithm", mergeAlgorithm))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-channelDyeSpec", channelDyeSpec))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-outputChannelOrder", outputChannelOrder))),
                                argThat(new ServiceArgMatcher(new ServiceArg("-generateMips", true)))
                        );

                        verify(updateSampleProcessingResultsProcessor).createServiceData(any(ServiceExecutionContext.class),
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
        when(sampleStitchResultHandler.getServiceDataResult(any(JacsServiceData.class)))
                .thenReturn(createSampleResult(
                        SampleProcessorTestUtils.TEST_SAMPLE_ID,
                        ImmutableList.of(
                                createSampleAreaResult(
                                        objective,
                                        area,
                                        "stitchedFile",
                                        ImmutableList.of(createMergeTilePairResult("t1", "f1"), createMergeTilePairResult("t2", "f2"))))));

        ServiceResultHandler<List<SampleProcessorResult>> updateSamplePipelineResultsResultHandler = mock(ServiceResultHandler.class);
        when(updateSampleProcessingResultsProcessor.getResultHandler()).thenReturn(updateSamplePipelineResultsResultHandler);
        when(updateSamplePipelineResultsResultHandler.getServiceDataResult(any(JacsServiceData.class)))
                .thenReturn(ImmutableList.of(createSampleProcessorResult(SampleProcessorTestUtils.TEST_SAMPLE_ID, objective, area)));

        ServiceResultHandler<MIPsAndMoviesResult> enhancedMIPsAndMoviesResultHandler = mock(ServiceResultHandler.class);
        when(enhancedMIPsAndMoviesProcessor.getResultHandler()).thenReturn(enhancedMIPsAndMoviesResultHandler);
        when(enhancedMIPsAndMoviesResultHandler.getServiceDataResult(any(JacsServiceData.class)))
                .thenReturn(createMipsAndMoviesResult(TEST_MIPS_DIR, ImmutableList.of("d1", "d2")));

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
                true, // persistResults (mipmaps)
                true, // run post sample processing neuron separation
                null, // no alignment algorithm
                null  // no alignment result
        );
        testServiceData.setId(Long.valueOf(testServiceId));

        ServiceComputation<JacsServiceResult<List<SampleProcessorResult>>> flylightProcessing = flylightSampleProcessor.process(testServiceData);
        Consumer successful = mock(Consumer.class);
        Consumer failure = mock(Consumer.class);
        flylightProcessing
                .thenApply(r -> {
                    successful.accept(r);

                    String expectedSampleRootDir = testSampleDir + "/" + SubjectUtils.getSubjectName(testServiceData.getOwnerKey());

                    verify(getSampleImageFilesProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", SampleProcessorTestUtils.TEST_SAMPLE_ID.toString()))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-area", area))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataRootDir", expectedSampleRootDir))),
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
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataRootDir", expectedSampleRootDir))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleLsmsSubDir", "Temp" + "/" + testLsmSubDir))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleSummarySubDir", "Summary" + "/" + testLsmSubDir))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleSitchingSubDir", "Sample" + "/" + testLsmSubDir))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-mergeAlgorithm", mergeAlgorithm))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-channelDyeSpec", channelDyeSpec))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-outputChannelOrder", outputChannelOrder))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-generateMips", true)))
                    );

                    verify(updateSampleProcessingResultsProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleResultsId", testServiceId))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleProcessingId", SampleProcessorTestUtils.TEST_SERVICE_ID.toString())))
                    );

                    verify(enhancedMIPsAndMoviesProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-imgFile", "f1"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-mode", "MCFO"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-chanSpec", "sssr"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-colorSpec", "RGB1"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-resultsDir", expectedSampleRootDir + "/" + "Post" + "/" + testLsmSubDir + "/" + objective + "/" + area))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-options", "mips:movies:legends:hist")))
                    );
                    verify(enhancedMIPsAndMoviesProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-imgFile", "f2"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-mode", "MCFO"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-chanSpec", "sssr"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-colorSpec", "RGB1"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-resultsDir", expectedSampleRootDir + "/" + "Post" + "/" + testLsmSubDir + "/" + objective + "/" + area))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-options", "mips:movies:legends:hist")))
                    );
                    verify(enhancedMIPsAndMoviesProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-imgFile", "stitchedFile"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-mode", "MCFO"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-chanSpec", "sssr"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-colorSpec", "RGB1"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-resultsDir", expectedSampleRootDir + "/" + "Post" + "/" + testLsmSubDir + "/" + objective + "/" + area))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-options", "mips:movies:legends:hist")))
                    );

                    verify(updateSamplePostProcessingResultsProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", SampleProcessorTestUtils.TEST_SAMPLE_ID))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleResultsId", testServiceId))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataRootDir", expectedSampleRootDir))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-samplePostSubDir", "Post" + "/" + testLsmSubDir))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-resultDirs", String.join(",", TEST_MIPS_DIR, TEST_MIPS_DIR, TEST_MIPS_DIR))))
                    );

                    verify(sampleNeuronSeparationProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", SampleProcessorTestUtils.TEST_SAMPLE_ID))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-runId", TEST_RUN_ID))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-resultId", TEST_RESULT_ID))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-inputFile", TEST_AREA_FILE))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-outputDir", expectedSampleRootDir + "/" + "Separation" + "/" + TEST_RESULT_ID + "/" + area))),
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

    @Test
    public void brainAndVncSampleProcessingSeparationAndNoAlignmentFor20x() {
        String objective = "20x";
        String mergeAlgorithm = "FLYLIGHT_ORDERED";
        String channelDyeSpec = "reference=Alexa Fluor 488,Cy2;" +
                "membrane_ha=,ATTO 647,Alexa Fluor 633,Alexa Fluor 647,Cy5;" +
                "membrane_v5=Alexa Fluor 546,Alexa Fluor 555,Alexa Fluor 568,DY-547;" +
                "membrane_flag=Alexa Fluor 594";
        String outputChannelOrder = "membrane_ha,membrane_v5,membrane_flag,reference";

        Sample testSample = SampleProcessorTestUtils.createTestSample(
                SampleProcessorTestUtils.TEST_SAMPLE_ID,
                objective,
                ImmutableList.of(
                        ImmutablePair.of("Brain", ImmutableList.of()),
                        ImmutablePair.of("VNC", ImmutableList.of())
                ));

        when(sampleDataService.getSampleById(argThat(argument -> {
            return argument == null || "testOwner".equals(argument);
        }), eq(SampleProcessorTestUtils.TEST_SAMPLE_ID))).thenReturn(testSample);

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
        when(sampleStitchResultHandler.getServiceDataResult(any(JacsServiceData.class)))
                .thenReturn(createSampleResult(
                        SampleProcessorTestUtils.TEST_SAMPLE_ID,
                        ImmutableList.of(
                                createSampleAreaResult(
                                        objective, "Brain", null, ImmutableList.of(createMergeTilePairResult("Brain", "fBrain"))
                                ),
                                createSampleAreaResult(
                                        objective, "VNC", null, ImmutableList.of(createMergeTilePairResult("VNC", "fVNC"))
                                )
                        )
                ));

        ServiceResultHandler<List<SampleProcessorResult>> updateSamplePipelineResultsResultHandler = mock(ServiceResultHandler.class);
        when(updateSampleProcessingResultsProcessor.getResultHandler()).thenReturn(updateSamplePipelineResultsResultHandler);
        when(updateSamplePipelineResultsResultHandler.getServiceDataResult(any(JacsServiceData.class)))
                .thenReturn(ImmutableList.of(
                        createSampleProcessorResult(SampleProcessorTestUtils.TEST_SAMPLE_ID, objective, "Brain"),
                        createSampleProcessorResult(SampleProcessorTestUtils.TEST_SAMPLE_ID, objective, "VNC")
                ));

        ServiceResultHandler<MIPsAndMoviesResult> basicMIPsAndMoviesResultHandler = mock(ServiceResultHandler.class);
        when(basicMIPsAndMoviesProcessor.getResultHandler()).thenReturn(basicMIPsAndMoviesResultHandler);
        when(basicMIPsAndMoviesResultHandler.getServiceDataResult(any(JacsServiceData.class)))
                .thenReturn(createMipsAndMoviesResult(TEST_MIPS_DIR, ImmutableList.of("d1")));

        ServiceResultHandler<NeuronSeparationFiles> sampleNeuronSeparationResultHandler = mock(ServiceResultHandler.class);
        when(sampleNeuronSeparationProcessor.getResultHandler()).thenReturn(sampleNeuronSeparationResultHandler);
        when(sampleNeuronSeparationResultHandler.getServiceDataResult(any(JacsServiceData.class))).thenReturn(new NeuronSeparationFiles());

        String testServiceId = "1234567891234567";
        String testLsmSubDir = "234/567/1234567891234567";
        String testSampleDir = SampleProcessorTestUtils.TEST_WORKING_DIR;
        JacsServiceData testServiceData = createTestServiceData(1L,
                SampleProcessorTestUtils.TEST_SAMPLE_ID,
                "",
                objective,
                mergeAlgorithm,
                channelDyeSpec,
                outputChannelOrder,
                true, // skipSummary
                true, // montageMipMaps
                true, // persistResults (mipmaps)
                true, // run post sample processing neuron separation
                null, // no alignment algorithm
                null  // no alignment result
        );
        testServiceData.setId(Long.valueOf(testServiceId));

        ServiceComputation<JacsServiceResult<List<SampleProcessorResult>>> flylightProcessing = flylightSampleProcessor.process(testServiceData);
        Consumer successful = mock(Consumer.class);
        Consumer failure = mock(Consumer.class);
        flylightProcessing
                .thenApply(r -> {
                    successful.accept(r);
                    String expectedSampleRootDir = testSampleDir + "/" + SubjectUtils.getSubjectName(testServiceData.getOwnerKey());

                    verify(getSampleImageFilesProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", SampleProcessorTestUtils.TEST_SAMPLE_ID.toString()))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-area", ""))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataRootDir", expectedSampleRootDir))),
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
                            argThat(new ServiceArgMatcher(new ServiceArg("-area", ""))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleResultsId", testServiceId))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataRootDir", expectedSampleRootDir))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleLsmsSubDir", "Temp" + "/" + testLsmSubDir))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleSummarySubDir", "Summary" + "/" + testLsmSubDir))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleSitchingSubDir", "Sample" + "/" + testLsmSubDir))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-mergeAlgorithm", mergeAlgorithm))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-channelDyeSpec", channelDyeSpec))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-outputChannelOrder", outputChannelOrder))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-generateMips", true)))
                    );

                    verify(updateSampleProcessingResultsProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleResultsId", testServiceId))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleProcessingId", SampleProcessorTestUtils.TEST_SERVICE_ID.toString())))
                    );

                    verify(basicMIPsAndMoviesProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-imgFile", "fBrain"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-imgFilePrefix", "testSample-Brain"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-secondImgFile", "fVNC"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-secondImgFilePrefix", "testSample-VNC"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-mode", "MCFO"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-chanSpec", "sssr"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-colorSpec", "RGB1"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-resultsDir", expectedSampleRootDir + "/" + "Post" + "/" + testLsmSubDir + "/" + objective + "/NormalizedBrainVNC"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-options", "mips:movies:legends:hist")))
                    );

                    verify(updateSamplePostProcessingResultsProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", SampleProcessorTestUtils.TEST_SAMPLE_ID))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleResultsId", testServiceId))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataRootDir", expectedSampleRootDir))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-samplePostSubDir", "Post" + "/" + testLsmSubDir))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-resultDirs", expectedSampleRootDir + "/" + "Post" + "/" + testLsmSubDir + "/" + objective + "/NormalizedBrainVNC")))
                    );

                    verify(sampleNeuronSeparationProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", SampleProcessorTestUtils.TEST_SAMPLE_ID))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-runId", TEST_RUN_ID))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-resultId", TEST_RESULT_ID))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-inputFile", TEST_AREA_FILE))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-outputDir", expectedSampleRootDir + "/" + "Separation" + "/" + TEST_RESULT_ID + "/Brain"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-signalChannels", "0 1"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-referenceChannel", "3"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-previousResultFile", "")))
                    );
                    verify(sampleNeuronSeparationProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", SampleProcessorTestUtils.TEST_SAMPLE_ID))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-runId", TEST_RUN_ID))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-resultId", TEST_RESULT_ID))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-inputFile", TEST_AREA_FILE))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-outputDir", expectedSampleRootDir + "/" + "Separation" + "/" + TEST_RESULT_ID + "/VNC"))),
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
                                                  boolean persistResults,
                                                  boolean runNeuronSeparationAfterSampleProcessing,
                                                  String alignmentAlgorithm,
                                                  String alignmentResult) {
        JacsServiceDataBuilder testServiceDataBuilder = new JacsServiceDataBuilder(null)
                .setOwnerKey("testOwner")
                .addArgs("-sampleId", String.valueOf(sampleId));
        if (StringUtils.isNotBlank(area)) {
            testServiceDataBuilder.addArgs("-area", area);
        }
        testServiceDataBuilder
                .addArgs("-objective", objective)
                .addArgs("-imageType", "MCFO")
                .addArgs("-sampleDataRootDir", SampleProcessorTestUtils.TEST_WORKING_DIR)
                .setWorkspace(SampleProcessorTestUtils.TEST_WORKING_DIR);

        if (StringUtils.isNotBlank(mergeAlgorithm))
            testServiceDataBuilder.addArgs("-mergeAlgorithm", mergeAlgorithm);

        if (StringUtils.isNotBlank(channelDyeSpec))
            testServiceDataBuilder.addArgs("-channelDyeSpec", channelDyeSpec);

        if (StringUtils.isNotBlank(outputChannelOrder))
            testServiceDataBuilder.addArgs("-outputChannelOrder", outputChannelOrder);

        if (skipSummary)
            testServiceDataBuilder.addArgs("-skipSummary");

        if (montageMipMaps)
            testServiceDataBuilder.addArgs("-montageMipMaps");

        if (persistResults)
            testServiceDataBuilder.addArgs("-persistResults");

        if (StringUtils.isNotBlank(alignmentAlgorithm))
            testServiceDataBuilder.addArgs("-alignmentAlgorithm", alignmentAlgorithm);

        if (StringUtils.isNotBlank(alignmentResult))
            testServiceDataBuilder.addArgs("-alignmentResultName", alignmentResult);

        if (runNeuronSeparationAfterSampleProcessing)
            testServiceDataBuilder.addArgs("-runNeuronSeparationAfterSampleProcessing");

        JacsServiceData testServiceData = testServiceDataBuilder.build();
        testServiceData.setId(serviceId);
        return testServiceData;
    }

    private SampleResult createSampleResult(Long sampleId, ImmutableList<SampleAreaResult> sars) {
        SampleResult sr = new SampleResult();
        sr.setSampleId(sampleId);
        sr.setSampleAreaResults(sars);
        return sr;
    }

    private SampleAreaResult createSampleAreaResult(String objective, String area,
                                                    String stichResult,
                                                    List<MergeTilePairResult> groupResults) {
        SampleAreaResult sar = new SampleAreaResult();
        sar.setSampleName("testSample");
        sar.setObjective(objective);
        sar.setAnatomicalArea(area);
        sar.setConsensusChannelComponents(createChannelComponents("sssr"));
        sar.setStichFile(stichResult);
        sar.setGroupResults(groupResults);
        return sar;
    }

    private MergeTilePairResult createMergeTilePairResult(String tileName, String mergeResultFile) {
        MergeTilePairResult mtpr = new MergeTilePairResult();
        mtpr.setTileName(tileName);
        mtpr.setMergeResultFile(mergeResultFile);
        mtpr.setChannelComponents(createChannelComponents("sssr"));
        return mtpr;
    }

    private ChannelComponents createChannelComponents(String channelSpec) {
        ChannelComponents cc = new ChannelComponents();
        cc.channelSpec = channelSpec;
        return cc;
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

    private MIPsAndMoviesResult createMipsAndMoviesResult(String resultsDir, List<String> fileList) {
        MIPsAndMoviesResult mipsAndMoviesResult = new MIPsAndMoviesResult();
        mipsAndMoviesResult.setResultsDir(resultsDir);
        mipsAndMoviesResult.setFileList(fileList);
        return mipsAndMoviesResult;
    }

}
