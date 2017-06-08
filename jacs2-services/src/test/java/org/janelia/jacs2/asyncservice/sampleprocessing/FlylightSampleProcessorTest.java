package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.alignservices.AlignmentServiceBuilderFactory;
import org.janelia.jacs2.asyncservice.alignservices.AlignmentProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationTestUtils;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgMatcher;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceDataBuilder;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.Map;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FlylightSampleProcessorTest {
    private static final String DEFAULT_MIP_MAPS_OPTIONS = "mips:movies:legends:bcomp";

    private GetSampleImageFilesProcessor getSampleImageFilesProcessor;
    private SampleLSMSummaryProcessor sampleLSMSummaryProcessor;
    private SampleStitchProcessor sampleStitchProcessor;
    private UpdateSamplePipelineResultsProcessor updateSamplePipelineResultsProcessor;
    private FlylightSampleProcessor flylightSampleProcessor;

    @Before
    public void setUp() {
        Logger logger = mock(Logger.class);

        ServiceComputationFactory computationFactory = ComputationTestUtils.createTestServiceComputationFactory(logger);

        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        SampleDataService sampleDataService = mock(SampleDataService.class);

        getSampleImageFilesProcessor = mock(GetSampleImageFilesProcessor.class);
        sampleLSMSummaryProcessor = mock(SampleLSMSummaryProcessor.class);
        sampleStitchProcessor = mock(SampleStitchProcessor.class);
        updateSamplePipelineResultsProcessor = mock(UpdateSamplePipelineResultsProcessor.class);
        SampleNeuronSeparationProcessor sampleNeuronSeparationProcessor = mock(SampleNeuronSeparationProcessor.class);
        AlignmentServiceBuilderFactory alignmentServiceBuilderFactory = mock(AlignmentServiceBuilderFactory.class);
        AlignmentProcessor alignmentProcessor = mock(AlignmentProcessor.class);
        UpdateAlignmentResultsProcessor updateAlignmentResultsProcessor = mock(UpdateAlignmentResultsProcessor.class);
        SampleNeuronWarpingProcessor sampleNeuronWarpingProcessor = mock(SampleNeuronWarpingProcessor.class);

        when(jacsServiceDataPersistence.findServiceHierarchy(any(Number.class))).then(invocation -> {
            JacsServiceData sd = new JacsServiceData();
            sd.setId(invocation.getArgument(0));
            return sd;
        });

        doAnswer(invocation -> {
            JacsServiceData jacsServiceData = invocation.getArgument(0);
            jacsServiceData.setId(SampleProcessorTestUtils.TEST_SERVICE_ID);
            jacsServiceData.setState(JacsServiceState.SUCCESSFUL); // mark the service as completed otherwise the computation doesn't return
            return null;
        }).when(jacsServiceDataPersistence).saveHierarchy(any(JacsServiceData.class));

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

        when(updateSamplePipelineResultsProcessor.getMetadata()).thenCallRealMethod();
        when(updateSamplePipelineResultsProcessor.createServiceData(any(ServiceExecutionContext.class),
                        any(ServiceArg.class)
                )
        ).thenCallRealMethod();

        flylightSampleProcessor = new FlylightSampleProcessor(computationFactory,
                jacsServiceDataPersistence,
                SampleProcessorTestUtils.TEST_WORKING_DIR,
                sampleDataService,
                getSampleImageFilesProcessor,
                sampleLSMSummaryProcessor,
                sampleStitchProcessor,
                updateSamplePipelineResultsProcessor,
                sampleNeuronSeparationProcessor,
                alignmentServiceBuilderFactory,
                alignmentProcessor,
                updateAlignmentResultsProcessor,
                sampleNeuronWarpingProcessor,
                logger);
    }

    @Test
    public void noSummaryNoSeparationAndNoAlignment() {
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

        int dataIndex = 0;
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
                    true,
                    true,
                    true
            );
            testServiceData.setId(testServiceId);
            // TODO
            dataIndex++;
        }
    }


    private JacsServiceData createTestServiceData(Long serviceId,
                                                  Long sampleId, String area, String objective,
                                                  String mergeAlgorithm,
                                                  String channelDyeSpec, String outputChannelOrder,
                                                  boolean montageMipMaps,
                                                  boolean useDistortionCorrection,
                                                  boolean persistResults) {
        JacsServiceDataBuilder testServiceDataBuilder = new JacsServiceDataBuilder(null)
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

        if (montageMipMaps)
            testServiceDataBuilder.addArg("-montageMipMaps");

        if (useDistortionCorrection)
            testServiceDataBuilder.addArg("-distortionCorrection");

        if (persistResults)
            testServiceDataBuilder.addArg("-persistResults");
        JacsServiceData testServiceData = testServiceDataBuilder.build();
        testServiceData.setId(serviceId);
        return testServiceData;
    }

}
