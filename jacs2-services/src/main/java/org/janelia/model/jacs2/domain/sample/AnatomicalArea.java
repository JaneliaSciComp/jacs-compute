package org.janelia.model.jacs2.domain.sample;

import java.util.ArrayList;
import java.util.List;

public class AnatomicalArea {

    private String datasetName;
    private Number sampleId;
    private String sampleName;
    private String sampleEffector;
    private String objective;
    private String name;
    private List<TileLsmPair> tileLsmPairs = new ArrayList<>();
    private String defaultChanSpec;

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public Number getSampleId() {
        return sampleId;
    }

    public void setSampleId(Number sampleId) {
        this.sampleId = sampleId;
    }

    public String getSampleName() {
        return sampleName;
    }

    public String getSampleEffector() {
        return sampleEffector;
    }

    public void setSampleEffector(String sampleEffector) {
        this.sampleEffector = sampleEffector;
    }

    public void setSampleName(String sampleName) {
        this.sampleName = sampleName;
    }

    public String getObjective() {
        return objective;
    }

    public void setObjective(String objective) {
        this.objective = objective;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<TileLsmPair> getTileLsmPairs() {
        return tileLsmPairs;
    }

    public void setTileLsmPairs(List<TileLsmPair> tileLsmPairs) {
        this.tileLsmPairs = tileLsmPairs;
    }

    public void addLsmPair(TileLsmPair tileLsmPair) {
        tileLsmPairs.add(tileLsmPair);
    }

    public String getDefaultChanSpec() {
        return defaultChanSpec;
    }

    public void setDefaultChanSpec(String defaultChanSpec) {
        this.defaultChanSpec = defaultChanSpec;
    }
}
