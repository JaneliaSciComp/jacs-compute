package org.janelia.jacs2.asyncservice.sampleprocessing;

class SampleIntermediateResult {
    private final Number childServiceId;

    SampleIntermediateResult(Number childServiceId) {
        this.childServiceId = childServiceId;
    }

    Number getChildServiceId() {
        return childServiceId;
    }
}
