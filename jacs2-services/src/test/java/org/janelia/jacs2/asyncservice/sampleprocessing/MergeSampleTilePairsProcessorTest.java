package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.it.jacs.model.domain.sample.AnatomicalArea;
import org.janelia.it.jacs.model.domain.sample.LSMImage;
import org.janelia.it.jacs.model.domain.sample.TileLsmPair;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.imageservices.Vaa3dChannelMapProcessor;
import org.janelia.jacs2.asyncservice.lsmfileservices.MergeLsmPairProcessor;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceDataBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.slf4j.Logger;

import java.nio.file.Paths;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MergeSampleTilePairsProcessorTest {

    private static final String TEST_WORKING_DIR = "testdir";
    private static final String TEST_TILE_NAME = "tileForTest";
    private static final String TEST_LSM1_METADATA = "src/test/resources/testdata/mergeTilePairs/lsm_1_1.metadata.json";
    private static final String TEST_LSM2_METADATA = "src/test/resources/testdata/mergeTilePairs/lsm_1_2.metadata.json";

    private static final Long TEST_ID = 1L;
    private static final Long TEST_SAMPLE_ID = 100L;

    private static class ServiceArgMatcher implements ArgumentMatcher<ServiceArg> {

        private ServiceArg matcher;

        public ServiceArgMatcher(ServiceArg matcher) {
            this.matcher = matcher;
        }

        @Override
        public boolean matches(ServiceArg argument) {
            return new EqualsBuilder().append(matcher.toStringArray(), argument.toStringArray()).build();
        }
    }

    private SampleDataService sampleDataService;
    private GetSampleLsmsMetadataProcessor getSampleLsmsMetadataProcessor;
    private MergeLsmPairProcessor mergeLsmPairProcessor;
    private Vaa3dChannelMapProcessor vaa3dChannelMapProcessor;
    private MergeSampleTilePairsProcessor mergeSampleTilePairsProcessor;

    @Before
    public void setUp() {
        ServiceComputationFactory computationFactory = mock(ServiceComputationFactory.class);
        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);

        getSampleLsmsMetadataProcessor = mock(GetSampleLsmsMetadataProcessor.class);
        mergeLsmPairProcessor = mock(MergeLsmPairProcessor.class);
        vaa3dChannelMapProcessor = mock(Vaa3dChannelMapProcessor.class);
        sampleDataService = mock(SampleDataService.class);
        Logger logger = mock(Logger.class);

        mergeSampleTilePairsProcessor = new MergeSampleTilePairsProcessor(computationFactory,
                jacsServiceDataPersistence,
                TEST_WORKING_DIR,
                getSampleLsmsMetadataProcessor,
                mergeLsmPairProcessor,
                vaa3dChannelMapProcessor,
                sampleDataService,
                logger);
    }

    @Test
    public void submitDependenciesWithDyeSpecThatDoesNotRequireReordering() {
        String objective = "objective";
        String area = "area";
        JacsServiceData testServiceData = createTestServiceData(TEST_SAMPLE_ID,
                "FLYLIGHT_ORDERED",
                "reference=Alexa Fluor 488,Cy2;" +
                        "membrane_ha=,ATTO 647,Alexa Fluor 633,Alexa Fluor 647,Cy5;" +
                        "membrane_v5=Alexa Fluor 546,Alexa Fluor 555,Alexa Fluor 568,DY-547;" +
                        "membrane_flag=Alexa Fluor 594",
                "membrane_ha,membrane_v5,membrane_flag,reference",
                objective,
                area);
        when(sampleDataService.getAnatomicalAreasBySampleIdObjectiveAndArea(null, TEST_SAMPLE_ID, objective, area))
                .thenReturn(ImmutableList.of(createTestAnatomicalArea("a1", null,
                        createTestLsmPair(
                                TEST_LSM1_METADATA, null, 0,
                                TEST_LSM2_METADATA, null, 0))));
        when(getSampleLsmsMetadataProcessor.createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        )).thenAnswer(invocation -> {
            JacsServiceData sd = new JacsServiceData();
            sd.setId(TEST_ID);
            return sd;
        });
        JacsServiceResult<MergeSampleTilePairsProcessor.MergeSampleTilePairsIntermediateResult> result = mergeSampleTilePairsProcessor.submitServiceDependencies(testServiceData);
        assertThat(result.getResult().getChannelMapping().channelMapping, equalTo("1,2,4,0"));
        assertThat(result.getResult().getChannelMapping().outputChannelComponents.channelSpec, equalTo("sssr"));
        assertThat(result.getResult().getChannelMapping().outputChannelComponents.signalChannelsPos, equalTo("0 1 2"));
        assertThat(result.getResult().getChannelMapping().outputChannelComponents.referenceChannelsPos, equalTo("3"));
        verify(mergeLsmPairProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm1", Paths.get(TEST_WORKING_DIR, "lsm1").toFile().getAbsolutePath()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm2", Paths.get(TEST_WORKING_DIR, "lsm2").toFile().getAbsolutePath()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope1", "m1"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope2", "m2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-distortionCorrection", false))),
                argThat(new ServiceArgMatcher(new ServiceArg("-multiscanVersion", "2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-output", TEST_WORKING_DIR)))
        );
        verify(vaa3dChannelMapProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        );
    }

    @Test
    public void submitDependenciesWithDyeSpecThatRequiresReordering() {
        String objective = "objective";
        String area = "area";
        JacsServiceData testServiceData = createTestServiceData(TEST_SAMPLE_ID,
                "FLYLIGHT_ORDERED",
                "reference=Alexa Fluor 488,Cy2;" +
                        "membrane_ha=,ATTO 647,Alexa Fluor 633,Alexa Fluor 647,Cy5;" +
                        "membrane_v5=Alexa Fluor 546,Alexa Fluor 555,Alexa Fluor 568,DY-547;" +
                        "membrane_flag=Alexa Fluor 594",
                "membrane_flag,membrane_ha,membrane_v5,reference",
                objective,
                area);
        when(sampleDataService.getAnatomicalAreasBySampleIdObjectiveAndArea(null, TEST_SAMPLE_ID, objective, area))
                .thenReturn(ImmutableList.of(createTestAnatomicalArea("a1", null,
                        createTestLsmPair(
                                TEST_LSM1_METADATA, null, 0,
                                TEST_LSM2_METADATA, null, 0))));
        when(getSampleLsmsMetadataProcessor.createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        )).thenAnswer(invocation -> {
            JacsServiceData sd = new JacsServiceData();
            sd.setId(TEST_ID);
            return sd;
        });
        JacsServiceResult<MergeSampleTilePairsProcessor.MergeSampleTilePairsIntermediateResult> result = mergeSampleTilePairsProcessor.submitServiceDependencies(testServiceData);
        assertThat(result.getResult().getChannelMapping().channelMapping, equalTo("4,1,2,0"));
        assertThat(result.getResult().getChannelMapping().outputChannelComponents.channelSpec, equalTo("sssr"));
        assertThat(result.getResult().getChannelMapping().outputChannelComponents.signalChannelsPos, equalTo("0 1 2"));
        assertThat(result.getResult().getChannelMapping().outputChannelComponents.referenceChannelsPos, equalTo("3"));
        verify(mergeLsmPairProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm1", Paths.get(TEST_WORKING_DIR, "lsm1").toFile().getAbsolutePath()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm2", Paths.get(TEST_WORKING_DIR, "lsm2").toFile().getAbsolutePath()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope1", "m1"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope2", "m2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-distortionCorrection", false))),
                argThat(new ServiceArgMatcher(new ServiceArg("-multiscanVersion", "2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-output", TEST_WORKING_DIR)))
        );
        verify(vaa3dChannelMapProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-input", TEST_WORKING_DIR + "/" + TEST_TILE_NAME + ".vaa3d"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-output", TEST_WORKING_DIR + "/" + TEST_TILE_NAME + ".vaa3d"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-channelMapping", "2,0,0,1,1,2,3,3")))
        );
    }

    @Test
    public void submitDependenciesWithChanSpecUsingFlylightOrderedAlgorithm() {
        String objective = "objective";
        String area = "area";
        JacsServiceData testServiceData = createTestServiceData(TEST_SAMPLE_ID,
                "FLYLIGHT_ORDERED",
                null,
                null,
                objective,
                area);
        when(sampleDataService.getAnatomicalAreasBySampleIdObjectiveAndArea(null, TEST_SAMPLE_ID, objective, area))
                .thenReturn(ImmutableList.of(createTestAnatomicalArea("a1", "sssr",
                        createTestLsmPair(
                                TEST_LSM1_METADATA, "ssr", 0,
                                TEST_LSM2_METADATA, "sr", 0))));
        when(getSampleLsmsMetadataProcessor.createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        )).thenAnswer(invocation -> {
            JacsServiceData sd = new JacsServiceData();
            sd.setId(TEST_ID);
            return sd;
        });
        JacsServiceResult<MergeSampleTilePairsProcessor.MergeSampleTilePairsIntermediateResult> result = mergeSampleTilePairsProcessor.submitServiceDependencies(testServiceData);
        assertThat(result.getResult().getChannelMapping().channelMapping, equalTo("0,1,3,2"));
        assertThat(result.getResult().getChannelMapping().outputChannelComponents.channelSpec, equalTo("sssr"));
        assertThat(result.getResult().getChannelMapping().outputChannelComponents.signalChannelsPos, equalTo("0 1 2"));
        assertThat(result.getResult().getChannelMapping().outputChannelComponents.referenceChannelsPos, equalTo("3"));
        verify(mergeLsmPairProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm1", Paths.get(TEST_WORKING_DIR, "lsm1").toFile().getAbsolutePath()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm2", Paths.get(TEST_WORKING_DIR, "lsm2").toFile().getAbsolutePath()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope1", "m1"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope2", "m2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-distortionCorrection", false))),
                argThat(new ServiceArgMatcher(new ServiceArg("-multiscanVersion", "2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-output", TEST_WORKING_DIR)))
        );
        verify(vaa3dChannelMapProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        );
    }

    @Test
    public void submitDependenciesWithChanSpecUsingFlylightAlgorithm() {
        String objective = "objective";
        String area = "area";
        JacsServiceData testServiceData = createTestServiceData(TEST_SAMPLE_ID,
                null,
                null,
                null,
                objective,
                area);
        when(sampleDataService.getAnatomicalAreasBySampleIdObjectiveAndArea(null, TEST_SAMPLE_ID, objective, area))
                .thenReturn(ImmutableList.of(createTestAnatomicalArea("a1", "sssr",
                        createTestLsmPair(
                                TEST_LSM1_METADATA, null, 3,
                                TEST_LSM2_METADATA, null, 2))));
        when(getSampleLsmsMetadataProcessor.createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        )).thenAnswer(invocation -> {
            JacsServiceData sd = new JacsServiceData();
            sd.setId(TEST_ID);
            return sd;
        });
        JacsServiceResult<MergeSampleTilePairsProcessor.MergeSampleTilePairsIntermediateResult> result = mergeSampleTilePairsProcessor.submitServiceDependencies(testServiceData);
        assertThat(result.getResult().getChannelMapping().channelMapping, equalTo("4,2,1,0"));
        assertThat(result.getResult().getChannelMapping().outputChannelComponents.channelSpec, equalTo("sssr"));
        assertThat(result.getResult().getChannelMapping().outputChannelComponents.signalChannelsPos, equalTo("0 1 2"));
        assertThat(result.getResult().getChannelMapping().outputChannelComponents.referenceChannelsPos, equalTo("3"));
        verify(mergeLsmPairProcessor).createServiceData(any(ServiceExecutionContext.class),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm1", Paths.get(TEST_WORKING_DIR, "lsm1").toFile().getAbsolutePath()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-lsm2", Paths.get(TEST_WORKING_DIR, "lsm2").toFile().getAbsolutePath()))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope1", "m1"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-microscope2", "m2"))),
                argThat(new ServiceArgMatcher(new ServiceArg("-distortionCorrection", false))),
                argThat(new ServiceArgMatcher(new ServiceArg("-multiscanVersion", null))),
                argThat(new ServiceArgMatcher(new ServiceArg("-output", TEST_WORKING_DIR)))
        );
        verify(vaa3dChannelMapProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        );
    }

    private JacsServiceData createTestServiceData(long sampleId, String mergeAlgorithm, String channelDyeSpec, String outputChannelOrder, String objective, String area) {
        JacsServiceDataBuilder testServiceDataBuilder = new JacsServiceDataBuilder(null)
                .addArg("-sampleId", String.valueOf(sampleId));

        if (StringUtils.isNotBlank(mergeAlgorithm))
            testServiceDataBuilder.addArg("-mergeAlgorithm", mergeAlgorithm);

        if (StringUtils.isNotBlank(channelDyeSpec))
            testServiceDataBuilder.addArg("-channelDyeSpec", channelDyeSpec);

        if (StringUtils.isNotBlank(outputChannelOrder))
            testServiceDataBuilder.addArg("-outputChannelOrder", outputChannelOrder);

        testServiceDataBuilder.addArg("-objective", objective)
                .addArg("-area", area)
                .addArg("-sampleDataDir", TEST_WORKING_DIR);
        return testServiceDataBuilder.build();
    }

    private AnatomicalArea createTestAnatomicalArea(String name, String chanSpec, TileLsmPair tp) {
        AnatomicalArea a = new AnatomicalArea();
        a.setDefaultChanSpec(chanSpec);
        a.setSampleId(TEST_SAMPLE_ID);
        a.setName(name);
        a.addLsmPair(tp);
        return a;
    }

    private TileLsmPair createTestLsmPair(String lsm1MetadataFile, String chanSpec1, int lsm1NChannels,
                                          String lsm2MetadataFile, String chanSpec2, int lsm2NChannels) {
        TileLsmPair tp = new TileLsmPair();
        tp.setTileName(TEST_TILE_NAME);
        tp.setFirstLsm(createTestLsm("lsm1", "m1", chanSpec1, lsm1NChannels, lsm1MetadataFile));
        tp.setSecondLsm(createTestLsm("lsm2", "m2", chanSpec2, lsm2NChannels, lsm2MetadataFile));
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