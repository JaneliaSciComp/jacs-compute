package org.janelia.jacs2.asyncservice.sampleprocessing;

import org.janelia.jacs2.asyncservice.common.ComputationTestUtils;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.imageservices.MIPGenerationProcessor;
import org.janelia.jacs2.asyncservice.imageservices.Vaa3dStitchAndBlendProcessor;
import org.janelia.jacs2.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.junit.Before;
import org.slf4j.Logger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlylightSampleProcessorTest {
    private static final String TEST_WORKING_DIR = "testdir";
    private static final Long TEST_ID = 1L;
    private static final Long TEST_SAMPLE_ID = 100L;

    private FlylightSampleProcessor flylightSampleProcessor;

    @Before
    public void setUp() {
        Logger logger = mock(Logger.class);

        ServiceComputationFactory computationFactory = ComputationTestUtils.createTestServiceComputationFactory(logger);

        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        TimebasedIdentifierGenerator idGenerator = mock(TimebasedIdentifierGenerator.class);

        SampleDataService sampleDataService = mock(SampleDataService.class);
        GetSampleImageFilesProcessor getSampleImageFilesProcessor = mock(GetSampleImageFilesProcessor.class);
        MergeAndGroupSampleTilePairsProcessor mergeAndGroupSampleTilePairsProcessor = mock(MergeAndGroupSampleTilePairsProcessor.class);
        Vaa3dStitchAndBlendProcessor vaa3dStitchAndBlendProcessor = mock(Vaa3dStitchAndBlendProcessor.class);
        MIPGenerationProcessor mipGenerationProcessor = mock(MIPGenerationProcessor.class);

        doAnswer(invocation -> {
            JacsServiceData jacsServiceData = invocation.getArgument(0);
            jacsServiceData.setId(TEST_ID);
            jacsServiceData.setState(JacsServiceState.SUCCESSFUL); // mark the service as completed otherwise the computation doesn't return
            return null;
        }).when(jacsServiceDataPersistence).saveHierarchy(any(JacsServiceData.class));

        when(idGenerator.generateId()).thenReturn(TEST_ID);

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

        flylightSampleProcessor = new FlylightSampleProcessor(computationFactory,
                jacsServiceDataPersistence,
                TEST_WORKING_DIR,
                sampleDataService,
                getSampleImageFilesProcessor,
                mergeAndGroupSampleTilePairsProcessor,
                vaa3dStitchAndBlendProcessor,
                mipGenerationProcessor,
                idGenerator,
                logger);
    }
}
