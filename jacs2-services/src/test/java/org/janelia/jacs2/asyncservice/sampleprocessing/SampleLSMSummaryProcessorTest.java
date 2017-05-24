package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.beans.HasPropertyWithValue;
import org.janelia.it.jacs.model.domain.sample.LSMImage;
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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SampleLSMSummaryProcessorTest {
    private static final String TEST_WORKING_DIR = "testdir";
    private static final String TEST_MIPS_DIR = "testmipsdir";
    private static final Long TEST_ID = 10L;
    private static final Long TEST_SAMPLE_ID = 100L;
    private static final String TEST_OWNER = "testOwner";

    private SampleDataService sampleDataService;
    private UpdateSampleLSMMetadataProcessor updateSampleLSMMetadataProcessor;
    private GetSampleMIPsAndMoviesProcessor getSampleMIPsAndMoviesProcessor;
    private SampleLSMSummaryProcessor sampleLSMSummaryProcessor;

    @Before
    public void setUp() {
        ServiceComputationFactory computationFactory = mock(ServiceComputationFactory.class);
        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        sampleDataService = mock(SampleDataService.class);
        updateSampleLSMMetadataProcessor = mock(UpdateSampleLSMMetadataProcessor.class);
        getSampleMIPsAndMoviesProcessor = mock(GetSampleMIPsAndMoviesProcessor.class);
        GroupAndMontageFolderImagesProcessor groupAndMontageFolderImagesProcessor = mock(GroupAndMontageFolderImagesProcessor.class);
        Logger logger = mock(Logger.class);

        when(jacsServiceDataPersistence.findServiceHierarchy(any(Number.class))).then(invocation -> {
            JacsServiceData sd = new JacsServiceData();
            sd.setId(invocation.getArgument(0));
            return sd;
        });

        doAnswer(invocation -> {
            JacsServiceData jacsServiceData = invocation.getArgument(0);
            jacsServiceData.setId(TEST_ID);
            return null;
        }).when(jacsServiceDataPersistence).saveHierarchy(any(JacsServiceData.class));

        when(jacsServiceDataPersistence.findById(any(Number.class))).thenAnswer(invocation -> {
            Number serviceId = invocation.getArgument(0);
            JacsServiceData sd = new JacsServiceData();
            sd.setId(serviceId);
            return sd;
        });

        when(groupAndMontageFolderImagesProcessor.getResultHandler()).thenCallRealMethod();

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
        JacsServiceData testServiceData = createTestServiceData(1L,
                TEST_SAMPLE_ID,
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

    private JacsServiceData createTestServiceData(Long serviceId, Long sampleId, String area, String objective, String channelDyeSpec) {
        JacsServiceDataBuilder testServiceDataBuilder = new JacsServiceDataBuilder(null)
                .setOwner(TEST_OWNER)
                .addArg("-sampleId", String.valueOf(sampleId))
                .addArg("-area", area)
                .addArg("-objective", objective)
                .addArg("-sampleDataDir", TEST_WORKING_DIR);
        if (StringUtils.isNotBlank(channelDyeSpec))
            testServiceDataBuilder.addArg("-channelDyeSpec", channelDyeSpec);

        JacsServiceData testService = testServiceDataBuilder.build();
        testService.setId(serviceId);
        return testService;
    }

    @Test
    public void updateServiceResult() {
        JacsServiceData testServiceData = createTestServiceData(1L,
                TEST_SAMPLE_ID,
                "area",
                "objective",
                null
        );
        JacsServiceResult<SampleLSMSummaryProcessor.SampleLSMSummaryIntermediateResult> testDependenciesResult = new JacsServiceResult<>(
                testServiceData,
                new SampleLSMSummaryProcessor.SampleLSMSummaryIntermediateResult(TEST_ID, TEST_ID + 1)
        );
        Long lsmId = TEST_ID;
        SampleLSMSummaryProcessor.MontageParameters montageParameters = createTestMontageParams(lsmId, TEST_ID + 1);
        testDependenciesResult.getResult().addMontage(montageParameters);
        LSMImage lsm = createLSMImage(lsmId);
        when(sampleDataService.getLSMsByIds(TEST_OWNER, ImmutableList.of(lsmId))).thenReturn(ImmutableList.of(lsm));

        JacsServiceResult<List<LSMSummary>> testResult = sampleLSMSummaryProcessor.updateServiceResult(testDependenciesResult);
        assertThat(
                testResult.getResult(),
                allOf(
                        hasItem(new HasPropertyWithValue<>("sampleImageFile", equalTo(montageParameters.montageData.getSampleImageFile()))),
                        hasItem(new HasPropertyWithValue<>("mipsResultsDir", equalTo(montageParameters.montageData.getMipsResultsDir())))
                )
        );
        verify(sampleDataService).updateLSMFiles(lsm);
    }

    @Test
    public void illegalStateWhenUpdateServiceResult() {
        JacsServiceData testServiceData = createTestServiceData(1L,
                TEST_SAMPLE_ID,
                "area",
                "objective",
                null
        );
        JacsServiceResult<SampleLSMSummaryProcessor.SampleLSMSummaryIntermediateResult> testDependenciesResult = new JacsServiceResult<>(
                testServiceData,
                new SampleLSMSummaryProcessor.SampleLSMSummaryIntermediateResult(TEST_ID, TEST_ID + 1)
        );
        Long lsmId = TEST_ID;
        SampleLSMSummaryProcessor.MontageParameters montageParameters = createTestMontageParams(lsmId, TEST_ID + 1);
        testDependenciesResult.getResult().addMontage(montageParameters);
        assertThatThrownBy(() -> sampleLSMSummaryProcessor.updateServiceResult(testDependenciesResult))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("No LSM image found for " + lsmId);
    }

    private LSMImage createLSMImage(Number lsmId) {
        LSMImage lsmImage = new LSMImage();
        lsmImage.setId(lsmId);
        return lsmImage;
    }

    private SampleLSMSummaryProcessor.MontageParameters createTestMontageParams(Long lsmId, Long montageServiceId) {
        SampleImageMIPsFile sif = new SampleImageMIPsFile();
        sif.setSampleImageFile(createSampleImage(lsmId));
        sif.setMipsResultsDir(TEST_MIPS_DIR);
        sif.setMips(ImmutableList.of("i1_signal.png", "i1_reference.png"));
        return new SampleLSMSummaryProcessor.MontageParameters(montageServiceId, sif);
    }

    private SampleImageFile createSampleImage(long lsmId) {
        SampleImageFile sif = new SampleImageFile();
        sif.setId(lsmId);
        return sif;
    }
}
