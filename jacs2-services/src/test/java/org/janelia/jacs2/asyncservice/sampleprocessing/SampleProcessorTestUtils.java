package org.janelia.jacs2.asyncservice.sampleprocessing;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.it.jacs.model.domain.Reference;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.it.jacs.model.domain.sample.AnatomicalArea;
import org.janelia.it.jacs.model.domain.sample.LSMImage;
import org.janelia.it.jacs.model.domain.sample.ObjectiveSample;
import org.janelia.it.jacs.model.domain.sample.Sample;
import org.janelia.it.jacs.model.domain.sample.SampleTile;
import org.janelia.it.jacs.model.domain.sample.TileLsmPair;

import java.util.List;

public class SampleProcessorTestUtils {

    static final Long TEST_SERVICE_ID = 1L;
    static final Long TEST_SAMPLE_ID = 100L;
    static final String TEST_WORKING_DIR = "testdir";
    static final String TEST_TILE_NAME = "tileForTest";
    static final String TEST_LSM_1 = "lsm1";
    static final String TEST_LSM1_METADATA = "src/test/resources/testdata/mergeTilePairs/lsm_1_1.metadata.json";
    static final String TEST_LSM_2 = "lsm2";
    static final String TEST_LSM2_METADATA = "src/test/resources/testdata/mergeTilePairs/lsm_1_2.metadata.json";

    private static volatile long nextId = 1;

    public static Sample createTestSample(Number sampleId) {
        Sample s = new Sample();
        s.setId(sampleId);
        return s;
    }

    public static ObjectiveSample createTestObjective(String objective) {
        ObjectiveSample objectiveSample = new ObjectiveSample();
        objectiveSample.setObjective(objective);
        return objectiveSample;
    }

    public static Sample createTestSample(Number sampleId, String objective, String area, TileLsmPair... tps) {
        Sample s = createTestSample(sampleId);
        s.addObjective(createTestObjective(objective, area, tps));
        return s;
    }

    public static Sample createTestSample(Number sampleId, String objective, List<Pair<String, List<TileLsmPair>>> areasWithTiles) {
        Sample s = createTestSample(sampleId);
        ObjectiveSample objectiveSample = createTestObjective(objective);
        for (Pair<String, List<TileLsmPair>> area : areasWithTiles) {
            for (TileLsmPair tile : area.getRight()) {
                objectiveSample.addTiles(tileFromTileLsmPair(area.getLeft(), tile));
            }
        }
        s.addObjective(objectiveSample);
        return s;
    }

    public static ObjectiveSample createTestObjective(String objective, String area, TileLsmPair... tps) {
        ObjectiveSample objectiveSample = createTestObjective(objective);
        for (TileLsmPair tileLsmPair : tps) {
            objectiveSample.addTiles(tileFromTileLsmPair(area, tileLsmPair));
        }
        return objectiveSample;
    }

    public static SampleTile tileFromTileLsmPair(String anatomicalArea, TileLsmPair tileLsmPair) {
        SampleTile st = new SampleTile();
        st.setAnatomicalArea(anatomicalArea);
        st.addLsmReference(Reference.createFor(tileLsmPair.getFirstLsm().getEntityRefId()));
        if (tileLsmPair.hasTwoLsms()) st.addLsmReference(Reference.createFor(tileLsmPair.getSecondLsm().getEntityRefId()));
        return st;
    }

    public static AnatomicalArea createTestAnatomicalArea(Number sampleId, String objective, String name, String chanSpec, TileLsmPair... tps) {
        AnatomicalArea a = new AnatomicalArea();
        a.setDefaultChanSpec(chanSpec);
        a.setSampleId(sampleId);
        a.setName(name);
        a.setObjective(objective);
        for (TileLsmPair tp : tps) {
            a.addLsmPair(tp);
        }
        return a;
    }

    public static TileLsmPair createTestLsmPair(String tileName,
                                                String lsm1Name, String lsm1MetadataFile, String chanSpec1, int lsm1NChannels,
                                                String lsm2Name, String lsm2MetadataFile, String chanSpec2, int lsm2NChannels) {
        TileLsmPair tp = new TileLsmPair();
        tp.setTileName(tileName);
        tp.setFirstLsm(createTestLsm(lsm1Name, "m1", chanSpec1, lsm1NChannels, lsm1MetadataFile));
        if (StringUtils.isNotBlank(lsm2Name)) tp.setSecondLsm(createTestLsm(lsm2Name, "m2", chanSpec2, lsm2NChannels, lsm2MetadataFile));
        return tp;
    }

    public static LSMImage createTestLsm(String lsmPath, String microscope, String chanSpec, int nChannels, String lsmMetadataFile) {
        LSMImage lsm = new LSMImage();
        lsm.setId(nextId++);
        lsm.setFilepath(lsmPath);
        lsm.setMicroscope(microscope);
        lsm.setChanSpec(chanSpec);
        lsm.setNumChannels(chanSpec != null ? chanSpec.length() : nChannels);
        lsm.setFileName(FileType.LsmMetadata, lsmMetadataFile);
        return lsm;
    }

}
