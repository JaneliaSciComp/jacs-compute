package org.janelia.jacs2.asyncservice.sampleprocessing;

import java.util.LinkedList;
import java.util.List;

class GetSampleLsmsIntermediateResult extends SampleIntermediateResult {
    final List<SampleImageFile> sampleImageFiles = new LinkedList<>();

    GetSampleLsmsIntermediateResult(Number getSampleLsmsServiceDataId) {
        super(getSampleLsmsServiceDataId);
    }

    void addSampleImageFile(SampleImageFile sif) {
        sampleImageFiles.add(sif);
    }
}
