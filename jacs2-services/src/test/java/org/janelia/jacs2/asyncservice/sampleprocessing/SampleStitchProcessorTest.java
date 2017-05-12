package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Matchers;
import org.janelia.it.jacs.model.domain.sample.AnatomicalArea;
import org.janelia.jacs2.asyncservice.common.ComputationTestUtils;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgMatcher;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.imageservices.MIPGenerationProcessor;
import org.janelia.jacs2.asyncservice.imageservices.Vaa3dStitchAndBlendProcessor;
import org.janelia.jacs2.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceDataBuilder;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.List;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SampleStitchProcessorTest {
    private SampleDataService sampleDataService;
    private GetSampleImageFilesProcessor getSampleImageFilesProcessor;
    private MergeAndGroupSampleTilePairsProcessor mergeAndGroupSampleTilePairsProcessor;
    private SampleStitchProcessor sampleStitchProcessor;

    @Before
    public void setUp() {
        Logger logger = mock(Logger.class);

        ServiceComputationFactory computationFactory = ComputationTestUtils.createTestServiceComputationFactory(logger);

        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        TimebasedIdentifierGenerator idGenerator = mock(TimebasedIdentifierGenerator.class);

        sampleDataService = mock(SampleDataService.class);
        getSampleImageFilesProcessor = mock(GetSampleImageFilesProcessor.class);
        mergeAndGroupSampleTilePairsProcessor = mock(MergeAndGroupSampleTilePairsProcessor.class);
        Vaa3dStitchAndBlendProcessor vaa3dStitchAndBlendProcessor = mock(Vaa3dStitchAndBlendProcessor.class);
        MIPGenerationProcessor mipGenerationProcessor = mock(MIPGenerationProcessor.class);

        doAnswer(invocation -> {
            JacsServiceData jacsServiceData = invocation.getArgument(0);
            jacsServiceData.setId(SampleProcessorTestUtils.TEST_SERVICE_ID);
            jacsServiceData.setState(JacsServiceState.SUCCESSFUL); // mark the service as completed otherwise the computation doesn't return
            return null;
        }).when(jacsServiceDataPersistence).saveHierarchy(any(JacsServiceData.class));

        when(idGenerator.generateId()).thenReturn(SampleProcessorTestUtils.TEST_SERVICE_ID);

        when(getSampleImageFilesProcessor.getMetadata()).thenCallRealMethod();
        when(getSampleImageFilesProcessor.createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        )).thenCallRealMethod();

        when(mergeAndGroupSampleTilePairsProcessor.getMetadata()).thenCallRealMethod();
        when(mergeAndGroupSampleTilePairsProcessor.createServiceData(any(ServiceExecutionContext.class),
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

        sampleStitchProcessor = new SampleStitchProcessor(computationFactory,
                jacsServiceDataPersistence,
                SampleProcessorTestUtils.TEST_WORKING_DIR,
                sampleDataService,
                getSampleImageFilesProcessor,
                mergeAndGroupSampleTilePairsProcessor,
                vaa3dStitchAndBlendProcessor,
                mipGenerationProcessor,
                idGenerator,
                logger);
    }

    @Test
    public void submitDependencies() {
        String area = "area";
        String objective = "objective";
        String mergeAlgorithm = "FLYLIGHT_ORDERED";
        String channelDyeSpec = "reference=Alexa Fluor 488,Cy2;" +
                "membrane_ha=,ATTO 647,Alexa Fluor 633,Alexa Fluor 647,Cy5;" +
                "membrane_v5=Alexa Fluor 546,Alexa Fluor 555,Alexa Fluor 568,DY-547;" +
                "membrane_flag=Alexa Fluor 594";
        String outputChannelOrder = "membrane_ha,membrane_v5,membrane_flag,reference";

        JacsServiceData testServiceData = createTestServiceData(SampleProcessorTestUtils.TEST_SAMPLE_ID,
                area,
                objective,
                mergeAlgorithm,
                channelDyeSpec,
                outputChannelOrder,
                true,
                true
        );

        AnatomicalArea testAnatomicalArea = SampleProcessorTestUtils.createTestAnatomicalArea(SampleProcessorTestUtils.TEST_SAMPLE_ID, objective, area, null,
                SampleProcessorTestUtils.createTestLsmPair(
                        SampleProcessorTestUtils.TEST_TILE_NAME,
                        SampleProcessorTestUtils.TEST_LSM_1, SampleProcessorTestUtils.TEST_LSM1_METADATA, null, 0,
                        SampleProcessorTestUtils.TEST_LSM_2, SampleProcessorTestUtils.TEST_LSM2_METADATA, null, 0),
                SampleProcessorTestUtils.createTestLsmPair(
                        SampleProcessorTestUtils.TEST_TILE_NAME,
                        SampleProcessorTestUtils.TEST_LSM_1, SampleProcessorTestUtils.TEST_LSM1_METADATA, null, 0,
                        SampleProcessorTestUtils.TEST_LSM_2, SampleProcessorTestUtils.TEST_LSM2_METADATA, null, 0),
                SampleProcessorTestUtils.createTestLsmPair(
                        SampleProcessorTestUtils.TEST_TILE_NAME,
                        SampleProcessorTestUtils.TEST_LSM_1, SampleProcessorTestUtils.TEST_LSM1_METADATA, null, 0,
                        SampleProcessorTestUtils.TEST_LSM_2, SampleProcessorTestUtils.TEST_LSM2_METADATA, null, 0)
        );
        when(sampleDataService.getAnatomicalAreasBySampleIdObjectiveAndArea(null, SampleProcessorTestUtils.TEST_SAMPLE_ID, objective, area))
                .thenReturn(ImmutableList.of(testAnatomicalArea, testAnatomicalArea));

        JacsServiceResult<SampleStitchProcessor.StitchProcessingIntermediateResult> result = sampleStitchProcessor.submitServiceDependencies(testServiceData);

        verify(getSampleImageFilesProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", SampleProcessorTestUtils.TEST_SAMPLE_ID.toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                argThat(new ServiceArgMatcher(new ServiceArg("-area", area))),
                argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataDir", SampleProcessorTestUtils.TEST_WORKING_DIR)))
        );

        int mergeInvocations = 2;

        verify(mergeAndGroupSampleTilePairsProcessor, times(mergeInvocations)).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", SampleProcessorTestUtils.TEST_SAMPLE_ID.toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                argThat(new ServiceArgMatcher(new ServiceArg("-area", area))),
                argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataDir", SampleProcessorTestUtils.TEST_WORKING_DIR))),
                argThat(new ServiceArgMatcher(new ServiceArg("-mergeAlgorithm", mergeAlgorithm))),
                argThat(new ServiceArgMatcher(new ServiceArg("-channelDyeSpec", channelDyeSpec))),
                argThat(new ServiceArgMatcher(new ServiceArg("-outputChannelOrder", outputChannelOrder))),
                argThat(new ServiceArgMatcher(new ServiceArg("-distortionCorrection", true)))
        );

        assertThat(result.getResult().sampleImageFiles, Matchers.empty());
        assertThat(result.getResult().getSampleLsmsServiceDataId, notNullValue());
        assertThat(result.getResult().getMergeTilePairServiceIds(), Matchers.hasSize(mergeInvocations));
    }

    @Test
    public void processingWhenThereAreMultipleTiles() {
        String area = "area";
        String objective = "objective";
        String mergeAlgorithm = "FLYLIGHT_ORDERED";
        String channelDyeSpec = "reference=Alexa Fluor 488,Cy2;" +
                "membrane_ha=,ATTO 647,Alexa Fluor 633,Alexa Fluor 647,Cy5;" +
                "membrane_v5=Alexa Fluor 546,Alexa Fluor 555,Alexa Fluor 568,DY-547;" +
                "membrane_flag=Alexa Fluor 594";
        String outputChannelOrder = "membrane_ha,membrane_v5,membrane_flag,reference";

        JacsServiceData testServiceData = createTestServiceData(SampleProcessorTestUtils.TEST_SAMPLE_ID,
                area,
                objective,
                mergeAlgorithm,
                channelDyeSpec,
                outputChannelOrder,
                true,
                true
        );

        Number getSampleLsmsServiceId = 10L;
        List<Number> mergeTilePairServiceIds = ImmutableList.of(11L, 12L, 13L);

        JacsServiceResult<SampleStitchProcessor.StitchProcessingIntermediateResult> intermediateResults = new JacsServiceResult<>(
                        testServiceData,
                        new SampleStitchProcessor.StitchProcessingIntermediateResult(getSampleLsmsServiceId, mergeTilePairServiceIds)
        );
        sampleStitchProcessor.processing(intermediateResults);
    }

    private JacsServiceData createTestServiceData(long sampleId, String area, String objective,
                                                  String mergeAlgorithm,
                                                  String channelDyeSpec, String outputChannelOrder,
                                                  boolean useDistortionCorrection,
                                                  boolean generateMips) {
        JacsServiceDataBuilder testServiceDataBuilder = new JacsServiceDataBuilder(null)
                .addArg("-sampleId", String.valueOf(sampleId))
                .addArg("-area", area)
                .addArg("-objective", objective)
                .addArg("-sampleDataDir", SampleProcessorTestUtils.TEST_WORKING_DIR)
                .setWorkspace(SampleProcessorTestUtils.TEST_WORKING_DIR);

        if (StringUtils.isNotBlank(mergeAlgorithm))
            testServiceDataBuilder.addArg("-mergeAlgorithm", mergeAlgorithm);

        if (StringUtils.isNotBlank(channelDyeSpec))
            testServiceDataBuilder.addArg("-channelDyeSpec", channelDyeSpec);

        if (StringUtils.isNotBlank(outputChannelOrder))
            testServiceDataBuilder.addArg("-outputChannelOrder", outputChannelOrder);

        if (useDistortionCorrection)
            testServiceDataBuilder.addArg("-distortionCorrection");

        if (generateMips)
            testServiceDataBuilder.addArg("-generateMips");
        return testServiceDataBuilder.build();
    }

}
