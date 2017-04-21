package org.janelia.jacs2.asyncservice.sampleprocessing;

import java.util.LinkedList;
import java.util.List;

class GetSampleLsmsIntermediateResult {
    final Number getSampleLsmsServiceDataId;
    final List<SampleImageFile> sampleImageFiles = new LinkedList<>();

    GetSampleLsmsIntermediateResult(Number getSampleLsmsServiceDataId) {
        this.getSampleLsmsServiceDataId = getSampleLsmsServiceDataId;
    }

    void addSampleImageFile(SampleImageFile sif) {
        sampleImageFiles.add(sif);
    }
}
