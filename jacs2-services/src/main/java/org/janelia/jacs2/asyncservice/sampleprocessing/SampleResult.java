package org.janelia.jacs2.asyncservice.sampleprocessing;

import java.util.List;

public class SampleResult {
    private Number sampleId;
    private List<SampleAreaResult> sampleAreaResults;

    public Number getSampleId() {
        return sampleId;
    }

    public void setSampleId(Number sampleId) {
        this.sampleId = sampleId;
    }

    public List<SampleAreaResult> getSampleAreaResults() {
        return sampleAreaResults;
    }

    public void setSampleAreaResults(List<SampleAreaResult> sampleAreaResults) {
        this.sampleAreaResults = sampleAreaResults;
    }
}
