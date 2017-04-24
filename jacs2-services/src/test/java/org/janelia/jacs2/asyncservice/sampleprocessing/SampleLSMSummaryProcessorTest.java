package org.janelia.jacs2.asyncservice.sampleprocessing;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgMatcher;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.imageservices.GroupAndMontageFolderImagesProcessor;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceDataBuilder;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SampleLSMSummaryProcessorTest {
    private static final String TEST_WORKING_DIR = "testdir";
    private static final Long TEST_ID = 10L;
    private static final Long TEST_SAMPLE_ID = 100L;

    private UpdateSampleLSMMetadataProcessor updateSampleLSMMetadataProcessor;
    private GetSampleMIPsAndMoviesProcessor getSampleMIPsAndMoviesProcessor;
    private SampleLSMSummaryProcessor sampleLSMSummaryProcessor;

    @Before
    public void setUp() {
        ServiceComputationFactory computationFactory = mock(ServiceComputationFactory.class);
        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        SampleDataService sampleDataService = mock(SampleDataService.class);
        updateSampleLSMMetadataProcessor = mock(UpdateSampleLSMMetadataProcessor.class);
        getSampleMIPsAndMoviesProcessor = mock(GetSampleMIPsAndMoviesProcessor.class);
        GroupAndMontageFolderImagesProcessor groupAndMontageFolderImagesProcessor = mock(GroupAndMontageFolderImagesProcessor.class);
        Logger logger = mock(Logger.class);

        doAnswer(invocation -> {
            JacsServiceData jacsServiceData = invocation.getArgument(0);
            jacsServiceData.setId(TEST_ID);
            return null;
        }).when(jacsServiceDataPersistence).saveHierarchy(any(JacsServiceData.class));

        sampleLSMSummaryProcessor = new SampleLSMSummaryProcessor(computationFactory,
                jacsServiceDataPersistence,
                TEST_WORKING_DIR,
                sampleDataService,
                updateSampleLSMMetadataProcessor,
                getSampleMIPsAndMoviesProcessor,
                groupAndMontageFolderImagesProcessor,
                logger);
    }

    @Test
    public void submitServiceDependencies() {
        String area = "area";
        String objective = "objective";
        JacsServiceData testServiceData = createTestServiceData(TEST_SAMPLE_ID,
                area,
                objective,
                null
        );

        when(updateSampleLSMMetadataProcessor.createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataDir", TEST_WORKING_DIR)))
        )).thenAnswer(invocation -> new JacsServiceData());

        when(getSampleMIPsAndMoviesProcessor.createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-options", "mips:movies:legends:bcomp"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataDir", TEST_WORKING_DIR)))
        )).thenAnswer(invocation -> new JacsServiceData());

        JacsServiceResult<SampleLSMSummaryProcessor.SampleLSMSummaryIntermediateResult> result = sampleLSMSummaryProcessor.submitServiceDependencies(testServiceData);
        assertThat(result.getResult().getSampleLsmsServiceDataId, equalTo(TEST_ID));
        assertThat(result.getResult().mipMapsServiceDataId, equalTo(TEST_ID));
        verify(updateSampleLSMMetadataProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", String.valueOf(TEST_SAMPLE_ID)))),
                argThat(new ServiceArgMatcher(new ServiceArg("-area", area))),
                argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                argThat(new ServiceArgMatcher(new ServiceArg("-channelDyeSpec", null))),
                argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataDir", TEST_WORKING_DIR)))
        );
        verify(getSampleMIPsAndMoviesProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-sampleId", String.valueOf(TEST_SAMPLE_ID)))),
                argThat(new ServiceArgMatcher(new ServiceArg("-area", area))),
                argThat(new ServiceArgMatcher(new ServiceArg("-objective", objective))),
                argThat(new ServiceArgMatcher(new ServiceArg("-options", "mips:movies:legends:bcomp"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-sampleDataDir", TEST_WORKING_DIR)))
        );
    }

    private JacsServiceData createTestServiceData(long sampleId, String area, String objective, String channelDyeSpec) {
        JacsServiceDataBuilder testServiceDataBuilder = new JacsServiceDataBuilder(null)
                .addArg("-sampleId", String.valueOf(sampleId))
                .addArg("-area", area)
                .addArg("-objective", objective)
                .addArg("-sampleDataDir", TEST_WORKING_DIR);
        if (StringUtils.isNotBlank(channelDyeSpec))
            testServiceDataBuilder.addArg("-channelDyeSpec", channelDyeSpec);

        return testServiceDataBuilder.build();
    }
}
