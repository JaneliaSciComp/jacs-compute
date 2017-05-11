package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.it.jacs.model.domain.sample.AnatomicalArea;
import org.janelia.it.jacs.model.domain.sample.LSMImage;
import org.janelia.it.jacs.model.domain.sample.TileLsmPair;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgMatcher;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.fileservices.LinkDataProcessor;
import org.janelia.jacs2.asyncservice.imageservices.Vaa3dChannelMapProcessor;
import org.janelia.jacs2.asyncservice.imageservices.Vaa3dStitchGroupingProcessor;
import org.janelia.jacs2.asyncservice.lsmfileservices.MergeLsmPairProcessor;
import org.janelia.jacs2.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceDataBuilder;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.nio.file.Paths;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MergeAndGroupSampleTilePairsProcessorTest {

    private static final String MERGE_DIRNAME = "merge";
    private static final String GROUP_DIRNAME = "group";

    private static final String TEST_WORKING_DIR = "testdir";
    private static final String TEST_TILE_NAME = "tileForTest";
    private static final String TEST_LSM_1 = "lsm1";
    private static final String TEST_LSM1_METADATA = "src/test/resources/testdata/mergeTilePairs/lsm_1_1.metadata.json";
    private static final String TEST_LSM_2 = "lsm2";
    private static final String TEST_LSM2_METADATA = "src/test/resources/testdata/mergeTilePairs/lsm_1_2.metadata.json";

    private static final Long TEST_ID = 1L;
    private static final Long TEST_SAMPLE_ID = 100L;

    private SampleDataService sampleDataService;
    private UpdateSampleLSMMetadataProcessor updateSampleLSMMetadataProcessor;
    private MergeLsmPairProcessor mergeLsmPairProcessor;
    private Vaa3dChannelMapProcessor vaa3dChannelMapProcessor;
    private LinkDataProcessor linkDataProcessor;
    private Vaa3dStitchGroupingProcessor vaa3dStitchGroupingProcessor;
    private MergeAndGroupSampleTilePairsProcessor mergeAndGroupSampleTilePairsProcessor;

    @Before
    public void setUp() {
        ServiceComputationFactory computationFactory = mock(ServiceComputationFactory.class);
        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        TimebasedIdentifierGenerator idGenerator = mock(TimebasedIdentifierGenerator.class);

        updateSampleLSMMetadataProcessor = mock(UpdateSampleLSMMetadataProcessor.class);
        mergeLsmPairProcessor = mock(MergeLsmPairProcessor.class);
        vaa3dChannelMapProcessor = mock(Vaa3dChannelMapProcessor.class);
        linkDataProcessor = mock(LinkDataProcessor.class);
        vaa3dStitchGroupingProcessor = mock(Vaa3dStitchGroupingProcessor.class);
        sampleDataService = mock(SampleDataService.class);

        Logger logger = mock(Logger.class);

        doAnswer(invocation -> {
            JacsServiceData jacsServiceData = invocation.getArgument(0);
            jacsServiceData.setId(TEST_ID);
            return null;
        }).when(jacsServiceDataPersistence).saveHierarchy(any(JacsServiceData.class));

        when(idGenerator.generateId()).thenReturn(TEST_ID);

        when(updateSampleLSMMetadataProcessor.getMetadata()).thenCallRealMethod();
        when(updateSampleLSMMetadataProcessor.createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        )).thenCallRealMethod();

        when(mergeLsmPairProcessor.getMetadata()).thenCallRealMethod();
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

        when(linkDataProcessor.getMetadata()).thenCallRealMethod();
        when(linkDataProcessor.createServiceData(any(ServiceExecutionContext.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class)
                )
        ).thenCallRealMethod();

        when(vaa3dChannelMapProcessor.getMetadata()).thenCallRealMethod();
        when(vaa3dChannelMapProcessor.createServiceData(any(ServiceExecutionContext.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class),
                        any(ServiceArg.class)
                )
        ).thenCallRealMethod();

        mergeAndGroupSampleTilePairsProcessor = new MergeAndGroupSampleTilePairsProcessor(computationFactory,
                jacsServiceDataPersistence,
                TEST_WORKING_DIR,
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
        JacsServiceData testServiceData = createTestServiceData(TEST_SAMPLE_ID,
                area,
                objective,
                "FLYLIGHT_ORDERED",
                "reference=Alexa Fluor 488,Cy2;" +
                        "membrane_ha=,ATTO 647,Alexa Fluor 633,Alexa Fluor 647,Cy5;" +
                        "membrane_v5=Alexa Fluor 546,Alexa Fluor 555,Alexa Fluor 568,DY-547;" +
                        "membrane_flag=Alexa Fluor 594",
                "membrane_ha,membrane_v5,membrane_flag,reference"
        );
        when(sampleDataService.getAnatomicalAreasBySampleIdObjectiveAndArea(null, TEST_SAMPLE_ID, objective, area))
                .thenReturn(ImmutableList.of(createTestAnatomicalArea(objective, area, null,
                        createTestLsmPair(
                                TEST_LSM1_METADATA, null, 0,
                                TEST_LSM2_METADATA, null, 0))));

        JacsServiceResult<MergeAndGroupSampleTilePairsProcessor.MergeSampleTilePairsIntermediateResult> result = mergeAndGroupSampleTilePairsProcessor.submitServiceDependencies(testServiceData);
        result.getResult().getAreasResults()
                .forEach(ar -> {
                    assertThat(ar.areaChannelMapping, equalTo("1,2,4,0"));
                    assertThat(ar.areaChannelComponents.channelSpec, equalTo("sssr"));
                    assertThat(ar.areaChannelComponents.signalChannelsPos, equalTo("0 1 2"));
                    assertThat(ar.areaChannelComponents.referenceChannelsPos, equalTo("3"));
                });
        verify(mergeLsmPairProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm1", Paths.get(TEST_WORKING_DIR, objective, area, "lsm1").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm2", Paths.get(TEST_WORKING_DIR, objective, area, "lsm2").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope1", "m1"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope2", "m2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-distortionCorrection", false))),
                argThat(new ServiceArgMatcher(new ServiceArg("-multiscanVersion", "2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-outputFile", Paths.get(TEST_WORKING_DIR, objective, area, MERGE_DIRNAME, "tile-" + TEST_TILE_NAME + ".v3draw").toString())))
        );
        verify(linkDataProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        );
        verify(vaa3dChannelMapProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        );
    }

    @Test
    public void submitDependenciesWithDyeSpecThatRequiresReordering() {
        String area = "area";
        String objective = "objective";
        JacsServiceData testServiceData = createTestServiceData(TEST_SAMPLE_ID,
                area,
                objective,
                "FLYLIGHT_ORDERED",
                "reference=Alexa Fluor 488,Cy2;" +
                        "membrane_ha=,ATTO 647,Alexa Fluor 633,Alexa Fluor 647,Cy5;" +
                        "membrane_v5=Alexa Fluor 546,Alexa Fluor 555,Alexa Fluor 568,DY-547;" +
                        "membrane_flag=Alexa Fluor 594",
                "membrane_flag,membrane_ha,membrane_v5,reference"
        );
        when(sampleDataService.getAnatomicalAreasBySampleIdObjectiveAndArea(null, TEST_SAMPLE_ID, objective, area))
                .thenReturn(ImmutableList.of(createTestAnatomicalArea(objective, area, null,
                        createTestLsmPair(
                                TEST_LSM1_METADATA, null, 0,
                                TEST_LSM2_METADATA, null, 0))));

        JacsServiceResult<MergeAndGroupSampleTilePairsProcessor.MergeSampleTilePairsIntermediateResult> result = mergeAndGroupSampleTilePairsProcessor.submitServiceDependencies(testServiceData);
        result.getResult().getAreasResults()
                .forEach(ar -> {
                    assertThat(ar.areaChannelMapping, equalTo("4,1,2,0"));
                    assertThat(ar.areaChannelComponents.channelSpec, equalTo("sssr"));
                    assertThat(ar.areaChannelComponents.signalChannelsPos, equalTo("0 1 2"));
                    assertThat(ar.areaChannelComponents.referenceChannelsPos, equalTo("3"));
                });
        verify(mergeLsmPairProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm1", Paths.get(TEST_WORKING_DIR, objective, area, "lsm1").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm2", Paths.get(TEST_WORKING_DIR, objective, area, "lsm2").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope1", "m1"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope2", "m2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-distortionCorrection", false))),
                argThat(new ServiceArgMatcher(new ServiceArg("-multiscanVersion", "2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-outputFile", Paths.get(TEST_WORKING_DIR, objective, area, MERGE_DIRNAME, "tile-" + TEST_TILE_NAME + ".v3draw").toString())))
        );
        verify(linkDataProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        );
        verify(vaa3dChannelMapProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-inputFile", Paths.get(TEST_WORKING_DIR, objective, area, MERGE_DIRNAME, "tile-" + TEST_TILE_NAME + ".v3draw").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-outputFile", Paths.get(TEST_WORKING_DIR, objective, area, MERGE_DIRNAME, "tile-" + TEST_TILE_NAME + ".v3draw").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-channelMapping", "2,0,0,1,1,2,3,3")))
        );
    }

    @Test
    public void submitDependenciesWithChanSpecUsingFlylightOrderedAlgorithm() {
        String area = "area";
        String objective = "objective";
        JacsServiceData testServiceData = createTestServiceData(TEST_SAMPLE_ID,
                area,
                objective,
                "FLYLIGHT_ORDERED",
                null,
                null
        );
        when(sampleDataService.getAnatomicalAreasBySampleIdObjectiveAndArea(null, TEST_SAMPLE_ID, objective, area))
                .thenReturn(ImmutableList.of(createTestAnatomicalArea(objective, area, "sssr",
                        createTestLsmPair(
                                TEST_LSM1_METADATA, "ssr", 0,
                                TEST_LSM2_METADATA, "sr", 0))));

        JacsServiceResult<MergeAndGroupSampleTilePairsProcessor.MergeSampleTilePairsIntermediateResult> result = mergeAndGroupSampleTilePairsProcessor.submitServiceDependencies(testServiceData);
        result.getResult().getAreasResults()
                .forEach(ar -> {
                    assertThat(ar.areaChannelMapping, equalTo("0,1,3,2"));
                    assertThat(ar.areaChannelComponents.channelSpec, equalTo("sssr"));
                    assertThat(ar.areaChannelComponents.signalChannelsPos, equalTo("0 1 2"));
                    assertThat(ar.areaChannelComponents.referenceChannelsPos, equalTo("3"));
                });
        verify(mergeLsmPairProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm1", Paths.get(TEST_WORKING_DIR, objective, area, "lsm1").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm2", Paths.get(TEST_WORKING_DIR, objective, area, "lsm2").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope1", "m1"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope2", "m2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-distortionCorrection", false))),
                argThat(new ServiceArgMatcher(new ServiceArg("-multiscanVersion", "2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-outputFile", Paths.get(TEST_WORKING_DIR, objective, area, MERGE_DIRNAME, "tile-" + TEST_TILE_NAME + ".v3draw").toString())))
        );
        verify(linkDataProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        );
        verify(vaa3dChannelMapProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        );
    }

    @Test
    public void submitDependenciesWithChanSpecUsingFlylightAlgorithm() {
        String area = "area";
        String objective = "objective";
        JacsServiceData testServiceData = createTestServiceData(TEST_SAMPLE_ID, area, objective, null, null, null);
        when(sampleDataService.getAnatomicalAreasBySampleIdObjectiveAndArea(null, TEST_SAMPLE_ID, objective, area))
                .thenReturn(ImmutableList.of(createTestAnatomicalArea(objective, area, "sssr",
                        createTestLsmPair(
                                TEST_LSM1_METADATA, null, 3,
                                TEST_LSM2_METADATA, null, 2))));

        JacsServiceResult<MergeAndGroupSampleTilePairsProcessor.MergeSampleTilePairsIntermediateResult> result = mergeAndGroupSampleTilePairsProcessor.submitServiceDependencies(testServiceData);
        result.getResult().getAreasResults()
                .forEach(ar -> {
                    assertThat(ar.areaChannelMapping, equalTo("4,2,1,0"));
                    assertThat(ar.areaChannelComponents.channelSpec, equalTo("sssr"));
                    assertThat(ar.areaChannelComponents.signalChannelsPos, equalTo("0 1 2"));
                    assertThat(ar.areaChannelComponents.referenceChannelsPos, equalTo("3"));
                });
        verify(mergeLsmPairProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm1", Paths.get(TEST_WORKING_DIR, objective, area, "lsm1").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm2", Paths.get(TEST_WORKING_DIR, objective, area, "lsm2").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope1", "m1"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope2", "m2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-distortionCorrection", false))),
                argThat(new ServiceArgMatcher(new ServiceArg("-multiscanVersion", null))),
                argThat(new ServiceArgMatcher(new ServiceArg("-outputFile", Paths.get(TEST_WORKING_DIR, objective, area, MERGE_DIRNAME, "tile-" + TEST_TILE_NAME + ".v3draw").toString())))
        );
        verify(linkDataProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        );
        verify(vaa3dChannelMapProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        );
    }

    @Test
    public void submitDependenciesWhenNoMergeIsNeeded() {
        String area = "area";
        String objective = "objective";
        JacsServiceData testServiceData = createTestServiceData(TEST_SAMPLE_ID, area, objective, null, null, null);
        when(sampleDataService.getAnatomicalAreasBySampleIdObjectiveAndArea(null, TEST_SAMPLE_ID, objective, area))
                .thenReturn(ImmutableList.of(createTestAnatomicalArea(objective, area, "rs",
                        createTestLsmPair(
                                TEST_LSM1_METADATA, null, 3,
                                null, null, 0))));

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
                argThat(new ServiceArgMatcher(new ServiceArg("-source", Paths.get(TEST_WORKING_DIR, objective, area, TEST_LSM_1).toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-target", Paths.get(TEST_WORKING_DIR, objective, area, MERGE_DIRNAME, "tile-" + TEST_TILE_NAME + ".v3draw").toString())))
        );
        verify(vaa3dChannelMapProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-inputFile", Paths.get(TEST_WORKING_DIR, objective, area, MERGE_DIRNAME, "tile-" + TEST_TILE_NAME + ".v3draw").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-outputFile", Paths.get(TEST_WORKING_DIR, objective, area, MERGE_DIRNAME, "tile-" + TEST_TILE_NAME + ".v3draw").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-channelMapping", "2,0,0,1")))
        );
    }

    @Test
    public void submitDependenciesWhenTileNamesAreNotUnique() {
        String area = "area";
        String objective = "objective";
        JacsServiceData testServiceData = createTestServiceData(TEST_SAMPLE_ID,
                area,
                objective,
                "FLYLIGHT_ORDERED",
                "reference=Alexa Fluor 488,Cy2;" +
                        "membrane_ha=,ATTO 647,Alexa Fluor 633,Alexa Fluor 647,Cy5;" +
                        "membrane_v5=Alexa Fluor 546,Alexa Fluor 555,Alexa Fluor 568,DY-547;" +
                        "membrane_flag=Alexa Fluor 594",
                "membrane_ha,membrane_v5,membrane_flag,reference"
        );
        AnatomicalArea testAnatomicalArea = createTestAnatomicalArea(objective, area, null,
                createTestLsmPair(
                        TEST_LSM1_METADATA, null, 0,
                        TEST_LSM2_METADATA, null, 0),
                createTestLsmPair(
                        TEST_LSM1_METADATA, null, 0,
                        TEST_LSM2_METADATA, null, 0),
                createTestLsmPair(
                        TEST_LSM1_METADATA, null, 0,
                        TEST_LSM2_METADATA, null, 0)
        );
        when(sampleDataService.getAnatomicalAreasBySampleIdObjectiveAndArea(null, TEST_SAMPLE_ID, objective, area))
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
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm1", Paths.get(TEST_WORKING_DIR, objective, area, "lsm1").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm2", Paths.get(TEST_WORKING_DIR, objective, area, "lsm2").toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope1", "m1"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope2", "m2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-distortionCorrection", false))),
                argThat(new ServiceArgMatcher(new ServiceArg("-multiscanVersion", "2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-outputFile", Paths.get(TEST_WORKING_DIR, objective, area, MERGE_DIRNAME, TEST_TILE_NAME + "tile-1.v3draw").toString())))
        );
        verify(linkDataProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        );
        verify(vaa3dChannelMapProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        );
        verify(vaa3dStitchGroupingProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-inputDir", Paths.get(TEST_WORKING_DIR, objective, area, MERGE_DIRNAME).toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-outputDir", Paths.get(TEST_WORKING_DIR, objective, area, GROUP_DIRNAME).toString()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-refchannel", "4")))
        );
    }

    private JacsServiceData createTestServiceData(long sampleId, String area, String objective, String mergeAlgorithm, String channelDyeSpec, String outputChannelOrder) {
        JacsServiceDataBuilder testServiceDataBuilder = new JacsServiceDataBuilder(null)
                .addArg("-sampleId", String.valueOf(sampleId))
                .addArg("-area", area)
                .addArg("-objective", objective)
                .addArg("-sampleDataDir", TEST_WORKING_DIR)
                .setWorkspace(TEST_WORKING_DIR);

        if (StringUtils.isNotBlank(mergeAlgorithm))
            testServiceDataBuilder.addArg("-mergeAlgorithm", mergeAlgorithm);

        if (StringUtils.isNotBlank(channelDyeSpec))
            testServiceDataBuilder.addArg("-channelDyeSpec", channelDyeSpec);

        if (StringUtils.isNotBlank(outputChannelOrder))
            testServiceDataBuilder.addArg("-outputChannelOrder", outputChannelOrder);

        return testServiceDataBuilder.build();
    }

    private AnatomicalArea createTestAnatomicalArea(String objective, String name, String chanSpec, TileLsmPair... tps) {
        AnatomicalArea a = new AnatomicalArea();
        a.setDefaultChanSpec(chanSpec);
        a.setSampleId(TEST_SAMPLE_ID);
        a.setName(name);
        a.setObjective(objective);
        for (TileLsmPair tp : tps) {
            a.addLsmPair(tp);
        }
        return a;
    }

    private TileLsmPair createTestLsmPair(String lsm1MetadataFile, String chanSpec1, int lsm1NChannels,
                                          String lsm2MetadataFile, String chanSpec2, int lsm2NChannels) {
        TileLsmPair tp = new TileLsmPair();
        tp.setTileName(TEST_TILE_NAME);
        tp.setFirstLsm(createTestLsm(TEST_LSM_1, "m1", chanSpec1, lsm1NChannels, lsm1MetadataFile));
        if (StringUtils.isNotBlank(lsm2MetadataFile)) tp.setSecondLsm(createTestLsm(TEST_LSM_2, "m2", chanSpec2, lsm2NChannels, lsm2MetadataFile));
        return tp;
    }

    private LSMImage createTestLsm(String lsmPath, String microscope, String chanSpec, int nChannels, String lsmMetadataFile) {
        LSMImage lsm = new LSMImage();
        lsm.setFilepath(lsmPath);
        lsm.setMicroscope(microscope);
        lsm.setChanSpec(chanSpec);
        lsm.setNumChannels(chanSpec != null ? chanSpec.length() : nChannels);
        lsm.setFileName(FileType.LsmMetadata, lsmMetadataFile);
        return lsm;
    }
}
