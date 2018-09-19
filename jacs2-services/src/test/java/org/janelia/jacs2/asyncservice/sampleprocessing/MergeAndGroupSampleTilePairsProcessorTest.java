package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.ServiceProcessorTestHelper;
import org.janelia.model.jacs2.domain.sample.AnatomicalArea;
import org.janelia.jacs2.asyncservice.common.ComputationTestHelper;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgMatcher;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.fileservices.LinkDataProcessor;
import org.janelia.jacs2.asyncservice.imageservices.Vaa3dChannelMapProcessor;
import org.janelia.jacs2.asyncservice.imageservices.Vaa3dStitchGroupingProcessor;
import org.janelia.jacs2.asyncservice.lsmfileservices.MergeLsmPairProcessor;
import org.janelia.model.access.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.janelia.model.service.JacsServiceState;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.nio.file.Paths;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MergeAndGroupSampleTilePairsProcessorTest {

    private static final String MERGE_DIRNAME = "merge";
    private static final String GROUP_DIRNAME = "group";

    private SampleDataService sampleDataService;
    private UpdateSampleLSMMetadataProcessor updateSampleLSMMetadataProcessor;
    private MergeLsmPairProcessor mergeLsmPairProcessor;
    private Vaa3dChannelMapProcessor vaa3dChannelMapProcessor;
    private LinkDataProcessor linkDataProcessor;
    private Vaa3dStitchGroupingProcessor vaa3dStitchGroupingProcessor;
    private MergeAndGroupSampleTilePairsProcessor mergeAndGroupSampleTilePairsProcessor;

    @Before
    public void setUp() {
        Logger logger = mock(Logger.class);

        ServiceComputationFactory computationFactory = ComputationTestHelper.createTestServiceComputationFactory(logger);

        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        TimebasedIdentifierGenerator idGenerator = mock(TimebasedIdentifierGenerator.class);

        updateSampleLSMMetadataProcessor = mock(UpdateSampleLSMMetadataProcessor.class);
        mergeLsmPairProcessor = mock(MergeLsmPairProcessor.class);
        vaa3dChannelMapProcessor = mock(Vaa3dChannelMapProcessor.class);
        linkDataProcessor = mock(LinkDataProcessor.class);
        vaa3dStitchGroupingProcessor = mock(Vaa3dStitchGroupingProcessor.class);
        sampleDataService = mock(SampleDataService.class);

        when(jacsServiceDataPersistence.findById(any(Number.class))).then(invocation -> {
            JacsServiceData sd = new JacsServiceData();
            sd.setId(invocation.getArgument(0));
            return sd;
        });

        when(jacsServiceDataPersistence.findDirectServiceDependencies(any(JacsServiceData.class))).then(invocation -> ImmutableList.of());


        when(jacsServiceDataPersistence.createServiceIfNotFound(any(JacsServiceData.class))).then(invocation -> {
            JacsServiceData jacsServiceData = invocation.getArgument(0);
            jacsServiceData.setId(SampleProcessorTestUtils.TEST_SERVICE_ID);
            jacsServiceData.setState(JacsServiceState.SUCCESSFUL); // mark the service as completed otherwise the computation doesn't return
            return jacsServiceData;
        });

        when(idGenerator.generateId()).thenReturn(SampleProcessorTestUtils.TEST_SERVICE_ID);

        ServiceProcessorTestHelper.prepareServiceProcessorMetadataAsRealCall(
                updateSampleLSMMetadataProcessor,
                mergeLsmPairProcessor,
                linkDataProcessor,
                vaa3dChannelMapProcessor
        );

        when(updateSampleLSMMetadataProcessor.createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        )).thenCallRealMethod();

        when(mergeLsmPairProcessor.createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
                )
        ).thenCallRealMethod();

        when(linkDataProcessor.createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
                )
        ).thenCallRealMethod();

        when(vaa3dChannelMapProcessor.createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
                )
        ).thenCallRealMethod();

        mergeAndGroupSampleTilePairsProcessor = new MergeAndGroupSampleTilePairsProcessor(computationFactory,
                jacsServiceDataPersistence,
                SampleProcessorTestUtils.TEST_WORKING_DIR,
                updateSampleLSMMetadataProcessor,
                mergeLsmPairProcessor,
                vaa3dChannelMapProcessor,
                linkDataProcessor,
                vaa3dStitchGroupingProcessor,
                sampleDataService,
                idGenerator,
                logger);
    }

    @Test
    public void submitDependenciesWithDyeSpecThatDoesNotRequireReordering() {
        String area = "area";
        String objective = "objective";
        JacsServiceData testServiceData = createTestServiceData(1L,
                SampleProcessorTestUtils.TEST_SAMPLE_ID,
                area,
                objective,
                "FLYLIGHT_ORDERED",
                "reference=Alexa Fluor 488,Cy2;" +
                        "membrane_ha=,ATTO 647,Alexa Fluor 633,Alexa Fluor 647,Cy5;" +
                        "membrane_v5=Alexa Fluor 546,Alexa Fluor 555,Alexa Fluor 568,DY-547;" +
                        "membrane_flag=Alexa Fluor 594",
                "membrane_ha,membrane_v5,membrane_flag,reference"
        );
        when(sampleDataService.getAnatomicalAreasBySampleIdObjectiveAndArea(null, SampleProcessorTestUtils.TEST_SAMPLE_ID, objective, area))
                .thenReturn(ImmutableList.of(SampleProcessorTestUtils.createTestAnatomicalArea(SampleProcessorTestUtils.TEST_SAMPLE_ID, objective, area, null,
                        SampleProcessorTestUtils.createTestLsmPair(
                                SampleProcessorTestUtils.TEST_TILE_NAME,
                                SampleProcessorTestUtils.TEST_LSM_1, SampleProcessorTestUtils.TEST_LSM1_METADATA, null, 0,
                                SampleProcessorTestUtils.TEST_LSM_2, SampleProcessorTestUtils.TEST_LSM2_METADATA, null, 0))));

        JacsServiceResult<MergeAndGroupSampleTilePairsProcessor.MergeSampleTilePairsIntermediateResult> result = mergeAndGroupSampleTilePairsProcessor.submitServiceDependencies(testServiceData);
        result.getResult().getAreasResults()
                .forEach(ar -> {
                    assertThat(ar.areaChannelMapping, equalTo("1,2,4,0"));
                    assertThat(ar.areaChannelComponents.channelSpec, equalTo("sssr"));
                    assertThat(ar.areaChannelComponents.signalChannelsPos, equalTo("0 1 2"));
                    assertThat(ar.areaChannelComponents.referenceChannelsPos, equalTo("3"));
                });
        verify(mergeLsmPairProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm1", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "lsms", objective, area, "lsm1").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm2", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "lsms", objective, area, "lsm2").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope1", "m1"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope2", "m2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-distortionCorrection", false))),
                argThat(new ServiceArgMatcher(new ServiceArg("-multiscanVersion", "2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-outputFile", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "stitching", objective, area, MERGE_DIRNAME, "tile-" + SampleProcessorTestUtils.TEST_TILE_NAME + ".v3draw").toString())))
        );
        verify(linkDataProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        );
        verify(vaa3dChannelMapProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-inputFile", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "stitching", objective, area, MERGE_DIRNAME, "tile-" + SampleProcessorTestUtils.TEST_TILE_NAME + ".v3draw").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-outputFile", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "stitching", objective, area, MERGE_DIRNAME, "tile-" + SampleProcessorTestUtils.TEST_TILE_NAME + ".v3draw").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-channelMapping", "0,0,1,1,2,2,3,3")))
        );
    }

    @Test
    public void submitDependenciesWithDyeSpecThatRequiresReordering() {
        String area = "area";
        String objective = "objective";
        JacsServiceData testServiceData = createTestServiceData(1L,
                SampleProcessorTestUtils.TEST_SAMPLE_ID,
                area,
                objective,
                "FLYLIGHT_ORDERED",
                "reference=Alexa Fluor 488,Cy2;" +
                        "membrane_ha=,ATTO 647,Alexa Fluor 633,Alexa Fluor 647,Cy5;" +
                        "membrane_v5=Alexa Fluor 546,Alexa Fluor 555,Alexa Fluor 568,DY-547;" +
                        "membrane_flag=Alexa Fluor 594",
                "membrane_flag,membrane_ha,membrane_v5,reference"
        );
        when(sampleDataService.getAnatomicalAreasBySampleIdObjectiveAndArea(null, SampleProcessorTestUtils.TEST_SAMPLE_ID, objective, area))
                .thenReturn(ImmutableList.of(SampleProcessorTestUtils.createTestAnatomicalArea(SampleProcessorTestUtils.TEST_SAMPLE_ID, objective, area, null,
                        SampleProcessorTestUtils.createTestLsmPair(
                                SampleProcessorTestUtils.TEST_TILE_NAME,
                                SampleProcessorTestUtils.TEST_LSM_1, SampleProcessorTestUtils.TEST_LSM1_METADATA, null, 0,
                                SampleProcessorTestUtils.TEST_LSM_2, SampleProcessorTestUtils.TEST_LSM2_METADATA, null, 0))));

        JacsServiceResult<MergeAndGroupSampleTilePairsProcessor.MergeSampleTilePairsIntermediateResult> result = mergeAndGroupSampleTilePairsProcessor.submitServiceDependencies(testServiceData);
        result.getResult().getAreasResults()
                .forEach(ar -> {
                    assertThat(ar.areaChannelMapping, equalTo("4,1,2,0"));
                    assertThat(ar.areaChannelComponents.channelSpec, equalTo("sssr"));
                    assertThat(ar.areaChannelComponents.signalChannelsPos, equalTo("0 1 2"));
                    assertThat(ar.areaChannelComponents.referenceChannelsPos, equalTo("3"));
                });
        verify(mergeLsmPairProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm1", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "lsms", objective, area, "lsm1").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm2", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "lsms", objective, area, "lsm2").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope1", "m1"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope2", "m2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-distortionCorrection", false))),
                argThat(new ServiceArgMatcher(new ServiceArg("-multiscanVersion", "2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-outputFile", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "stitching", objective, area, MERGE_DIRNAME, "tile-" + SampleProcessorTestUtils.TEST_TILE_NAME + ".v3draw").toString())))
        );
        verify(linkDataProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        );
        verify(vaa3dChannelMapProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-inputFile", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "stitching", objective, area, MERGE_DIRNAME, "tile-" + SampleProcessorTestUtils.TEST_TILE_NAME + ".v3draw").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-outputFile", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "stitching", objective, area, MERGE_DIRNAME, "tile-" + SampleProcessorTestUtils.TEST_TILE_NAME + ".v3draw").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-channelMapping", "2,0,0,1,1,2,3,3")))
        );
    }

    @Test
    public void submitDependenciesWithChanSpecUsingFlylightOrderedAlgorithm() {
        String area = "area";
        String objective = "objective";
        JacsServiceData testServiceData = createTestServiceData(1L,
                SampleProcessorTestUtils.TEST_SAMPLE_ID,
                area,
                objective,
                "FLYLIGHT_ORDERED",
                null,
                null
        );
        when(sampleDataService.getAnatomicalAreasBySampleIdObjectiveAndArea(null, SampleProcessorTestUtils.TEST_SAMPLE_ID, objective, area))
                .thenReturn(ImmutableList.of(SampleProcessorTestUtils.createTestAnatomicalArea(SampleProcessorTestUtils.TEST_SAMPLE_ID, objective, area, "sssr",
                        SampleProcessorTestUtils.createTestLsmPair(
                                SampleProcessorTestUtils.TEST_TILE_NAME,
                                SampleProcessorTestUtils.TEST_LSM_1, SampleProcessorTestUtils.TEST_LSM1_METADATA, "ssr", 0,
                                SampleProcessorTestUtils.TEST_LSM_2, SampleProcessorTestUtils.TEST_LSM2_METADATA, "sr", 0))));

        JacsServiceResult<MergeAndGroupSampleTilePairsProcessor.MergeSampleTilePairsIntermediateResult> result = mergeAndGroupSampleTilePairsProcessor.submitServiceDependencies(testServiceData);
        result.getResult().getAreasResults()
                .forEach(ar -> {
                    assertThat(ar.areaChannelMapping, equalTo("0,1,3,2"));
                    assertThat(ar.areaChannelComponents.channelSpec, equalTo("sssr"));
                    assertThat(ar.areaChannelComponents.signalChannelsPos, equalTo("0 1 2"));
                    assertThat(ar.areaChannelComponents.referenceChannelsPos, equalTo("3"));
                });
        verify(mergeLsmPairProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm1", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "lsms", objective, area, "lsm1").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm2", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "lsms", objective, area, "lsm2").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope1", "m1"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope2", "m2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-distortionCorrection", false))),
                argThat(new ServiceArgMatcher(new ServiceArg("-multiscanVersion", "2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-outputFile", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "stitching", objective, area, MERGE_DIRNAME, "tile-" + SampleProcessorTestUtils.TEST_TILE_NAME + ".v3draw").toString())))
        );
        verify(linkDataProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        );
        verify(vaa3dChannelMapProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-inputFile", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "stitching", objective, area, MERGE_DIRNAME, "tile-" + SampleProcessorTestUtils.TEST_TILE_NAME + ".v3draw").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-outputFile", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "stitching", objective, area, MERGE_DIRNAME, "tile-" + SampleProcessorTestUtils.TEST_TILE_NAME + ".v3draw").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-channelMapping", "0,0,1,1,2,2,3,3")))
        );
    }

    @Test
    public void submitDependenciesWithChanSpecUsingFlylightOrderedAlgorithmWhenRefChannelIsFirst() {
        String area = "area";
        String objective = "objective";
        JacsServiceData testServiceData = createTestServiceData(1L,
                SampleProcessorTestUtils.TEST_SAMPLE_ID,
                area,
                objective,
                "FLYLIGHT_ORDERED",
                null,
                null
        );
        when(sampleDataService.getAnatomicalAreasBySampleIdObjectiveAndArea(null, SampleProcessorTestUtils.TEST_SAMPLE_ID, objective, area))
                .thenReturn(ImmutableList.of(SampleProcessorTestUtils.createTestAnatomicalArea(SampleProcessorTestUtils.TEST_SAMPLE_ID, objective, area, "rsss",
                        SampleProcessorTestUtils.createTestLsmPair(
                                SampleProcessorTestUtils.TEST_TILE_NAME,
                                SampleProcessorTestUtils.TEST_LSM_1, SampleProcessorTestUtils.TEST_LSM1_METADATA, "ssr", 0,
                                SampleProcessorTestUtils.TEST_LSM_2, SampleProcessorTestUtils.TEST_LSM2_METADATA, "sr", 0))));

        JacsServiceResult<MergeAndGroupSampleTilePairsProcessor.MergeSampleTilePairsIntermediateResult> result = mergeAndGroupSampleTilePairsProcessor.submitServiceDependencies(testServiceData);
        result.getResult().getAreasResults()
                .forEach(ar -> {
                    assertThat(ar.areaChannelMapping, equalTo("2,0,1,3"));
                    assertThat(ar.areaChannelComponents.channelSpec, equalTo("rsss"));
                    assertThat(ar.areaChannelComponents.signalChannelsPos, equalTo("1 2 3"));
                    assertThat(ar.areaChannelComponents.referenceChannelsPos, equalTo("0"));
                });
        verify(mergeLsmPairProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm1", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "lsms", objective, area, "lsm1").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm2", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "lsms", objective, area, "lsm2").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope1", "m1"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope2", "m2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-distortionCorrection", false))),
                argThat(new ServiceArgMatcher(new ServiceArg("-multiscanVersion", "2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-outputFile", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "stitching", objective, area, MERGE_DIRNAME, "tile-" + SampleProcessorTestUtils.TEST_TILE_NAME + ".v3draw").toString())))
        );
        verify(linkDataProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        );
        verify(vaa3dChannelMapProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-inputFile", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "stitching", objective, area, MERGE_DIRNAME, "tile-" + SampleProcessorTestUtils.TEST_TILE_NAME + ".v3draw").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-outputFile", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "stitching", objective, area, MERGE_DIRNAME, "tile-" + SampleProcessorTestUtils.TEST_TILE_NAME + ".v3draw").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-channelMapping", "3,0,0,1,1,2,2,3")))
        );
    }

    @Test
    public void submitDependenciesWithChanSpecUsingFlylightAlgorithm() {
        String area = "area";
        String objective = "objective";
        JacsServiceData testServiceData = createTestServiceData(1L, SampleProcessorTestUtils.TEST_SAMPLE_ID, area, objective, null, null, null);
        when(sampleDataService.getAnatomicalAreasBySampleIdObjectiveAndArea(null, SampleProcessorTestUtils.TEST_SAMPLE_ID, objective, area))
                .thenReturn(ImmutableList.of(SampleProcessorTestUtils.createTestAnatomicalArea(SampleProcessorTestUtils.TEST_SAMPLE_ID, objective, area, "sssr",
                        SampleProcessorTestUtils.createTestLsmPair(
                                SampleProcessorTestUtils.TEST_TILE_NAME,
                                SampleProcessorTestUtils.TEST_LSM_1, SampleProcessorTestUtils.TEST_LSM1_METADATA, null, 3,
                                SampleProcessorTestUtils.TEST_LSM_2, SampleProcessorTestUtils.TEST_LSM2_METADATA, null, 2))));

        JacsServiceResult<MergeAndGroupSampleTilePairsProcessor.MergeSampleTilePairsIntermediateResult> result = mergeAndGroupSampleTilePairsProcessor.submitServiceDependencies(testServiceData);
        result.getResult().getAreasResults()
                .forEach(ar -> {
                    assertThat(ar.areaChannelMapping, equalTo("4,2,1,0"));
                    assertThat(ar.areaChannelComponents.channelSpec, equalTo("sssr"));
                    assertThat(ar.areaChannelComponents.signalChannelsPos, equalTo("0 1 2"));
                    assertThat(ar.areaChannelComponents.referenceChannelsPos, equalTo("3"));
                });
        verify(mergeLsmPairProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm1", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "lsms", objective, area, "lsm1").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm2", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "lsms", objective, area, "lsm2").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope1", "m1"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope2", "m2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-distortionCorrection", false))),
                argThat(new ServiceArgMatcher(new ServiceArg("-multiscanVersion", ""))),
                argThat(new ServiceArgMatcher(new ServiceArg("-outputFile", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "stitching", objective, area, MERGE_DIRNAME, "tile-" + SampleProcessorTestUtils.TEST_TILE_NAME + ".v3draw").toString())))
        );
        verify(linkDataProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        );
        verify(vaa3dChannelMapProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-inputFile", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "stitching", objective, area, MERGE_DIRNAME, "tile-" + SampleProcessorTestUtils.TEST_TILE_NAME + ".v3draw").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-outputFile", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "stitching", objective, area, MERGE_DIRNAME, "tile-" + SampleProcessorTestUtils.TEST_TILE_NAME + ".v3draw").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-channelMapping", "0,0,1,1,2,2,3,3")))
        );
    }

    @Test
    public void submitDependenciesWhenNoMergeIsNeeded() {
        String area = "area";
        String objective = "objective";
        JacsServiceData testServiceData = createTestServiceData(1L, SampleProcessorTestUtils.TEST_SAMPLE_ID, area, objective, null, null, null);
        when(sampleDataService.getAnatomicalAreasBySampleIdObjectiveAndArea(null, SampleProcessorTestUtils.TEST_SAMPLE_ID, objective, area))
                .thenReturn(ImmutableList.of(SampleProcessorTestUtils.createTestAnatomicalArea(SampleProcessorTestUtils.TEST_SAMPLE_ID, objective, area, "rs",
                        SampleProcessorTestUtils.createTestLsmPair(
                                SampleProcessorTestUtils.TEST_TILE_NAME,
                                SampleProcessorTestUtils.TEST_LSM_1, SampleProcessorTestUtils.TEST_LSM1_METADATA, null, 3,
                                null, null, null, 0))));

        JacsServiceResult<MergeAndGroupSampleTilePairsProcessor.MergeSampleTilePairsIntermediateResult> result = mergeAndGroupSampleTilePairsProcessor.submitServiceDependencies(testServiceData);
        result.getResult().getAreasResults()
                .forEach(ar -> {
                    assertThat(ar.areaChannelMapping, equalTo("0,1"));
                    assertThat(ar.areaChannelComponents.channelSpec, equalTo("rs"));
                    assertThat(ar.areaChannelComponents.signalChannelsPos, equalTo("1"));
                    assertThat(ar.areaChannelComponents.referenceChannelsPos, equalTo("0"));
                });
        verify(mergeLsmPairProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        );
        verify(linkDataProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-source", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "lsms", objective, area, SampleProcessorTestUtils.TEST_LSM_1).toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-target", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "stitching", objective, area, MERGE_DIRNAME, SampleProcessorTestUtils.TEST_LSM_1).toString())))
        );
        verify(vaa3dChannelMapProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-inputFile", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "stitching", objective, area, MERGE_DIRNAME, SampleProcessorTestUtils.TEST_LSM_1).toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-outputFile", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "stitching", objective, area, MERGE_DIRNAME, "tile-" + SampleProcessorTestUtils.TEST_TILE_NAME + ".v3draw").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-channelMapping", "2,0,0,1")))
        );
    }

    @Test
    public void submitDependenciesWhenTileNamesAreNotUnique() {
        String area = "area";
        String objective = "objective";
        JacsServiceData testServiceData = createTestServiceData(1L,
                SampleProcessorTestUtils.TEST_SAMPLE_ID,
                area,
                objective,
                "FLYLIGHT_ORDERED",
                "reference=Alexa Fluor 488,Cy2;" +
                        "membrane_ha=,ATTO 647,Alexa Fluor 633,Alexa Fluor 647,Cy5;" +
                        "membrane_v5=Alexa Fluor 546,Alexa Fluor 555,Alexa Fluor 568,DY-547;" +
                        "membrane_flag=Alexa Fluor 594",
                "membrane_ha,membrane_v5,membrane_flag,reference"
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
                .thenReturn(ImmutableList.of(testAnatomicalArea));

        JacsServiceResult<MergeAndGroupSampleTilePairsProcessor.MergeSampleTilePairsIntermediateResult> result = mergeAndGroupSampleTilePairsProcessor.submitServiceDependencies(testServiceData);
        result.getResult().getAreasResults()
                .forEach(ar -> {
                    assertThat(ar.areaChannelMapping, equalTo("1,2,4,0"));
                    assertThat(ar.areaChannelComponents.channelSpec, equalTo("sssr"));
                    assertThat(ar.areaChannelComponents.signalChannelsPos, equalTo("0 1 2"));
                    assertThat(ar.areaChannelComponents.referenceChannelsPos, equalTo("3"));
                });
        verify(mergeLsmPairProcessor, times(testAnatomicalArea.getTileLsmPairs().size())).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm1", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "lsms", objective, area, "lsm1").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm2", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "lsms", objective, area, "lsm2").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope1", "m1"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope2", "m2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-distortionCorrection", false))),
                argThat(new ServiceArgMatcher(new ServiceArg("-multiscanVersion", "2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-outputFile", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "stitching", objective, area, MERGE_DIRNAME, SampleProcessorTestUtils.TEST_TILE_NAME + "tile-1.v3draw").toString())))
        );
        verify(linkDataProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        );
        verify(vaa3dChannelMapProcessor, times(testAnatomicalArea.getTileLsmPairs().size())).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-inputFile", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "stitching", objective, area, MERGE_DIRNAME, SampleProcessorTestUtils.TEST_TILE_NAME + "tile-1.v3draw").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-outputFile", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "stitching", objective, area, MERGE_DIRNAME, SampleProcessorTestUtils.TEST_TILE_NAME + "tile-1.v3draw").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-channelMapping", "0,0,1,1,2,2,3,3")))
        );
        verify(vaa3dStitchGroupingProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-inputDir", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "stitching", objective, area, MERGE_DIRNAME).toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-outputDir", Paths.get(SampleProcessorTestUtils.TEST_WORKING_DIR, "stitching", objective, area, GROUP_DIRNAME).toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-refchannel", "4")))
        );
    }

    private JacsServiceData createTestServiceData(Long serviceId, Long sampleId, String area, String objective, String mergeAlgorithm, String channelDyeSpec, String outputChannelOrder) {
        JacsServiceDataBuilder testServiceDataBuilder = new JacsServiceDataBuilder(null)
                .addArgs("-sampleId", String.valueOf(sampleId))
                .addArgs("-area", area)
                .addArgs("-objective", objective)
                .addArgs("-sampleDataRootDir", SampleProcessorTestUtils.TEST_WORKING_DIR)
                .addArgs("-sampleLsmsSubDir", "lsms")
                .addArgs("-sampleSummarySubDir", "summary")
                .addArgs("-sampleSitchingSubDir", "stitching")
                .setWorkspace(SampleProcessorTestUtils.TEST_WORKING_DIR);

        if (StringUtils.isNotBlank(mergeAlgorithm))
            testServiceDataBuilder.addArgs("-mergeAlgorithm", mergeAlgorithm);

        if (StringUtils.isNotBlank(channelDyeSpec))
            testServiceDataBuilder.addArgs("-channelDyeSpec", channelDyeSpec);

        if (StringUtils.isNotBlank(outputChannelOrder))
            testServiceDataBuilder.addArgs("-outputChannelOrder", outputChannelOrder);

        JacsServiceData testServiceData = testServiceDataBuilder.build();
        testServiceData.setId(serviceId);
        return testServiceData;
    }

}
