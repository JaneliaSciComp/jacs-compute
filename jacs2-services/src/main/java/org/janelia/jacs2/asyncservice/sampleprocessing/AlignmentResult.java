package org.janelia.jacs2.asyncservice.sampleprocessing;

import org.janelia.model.jacs2.domain.sample.SampleAlignmentResult;
import org.janelia.jacs2.asyncservice.alignservices.AlignmentResultFiles;

public class AlignmentResult {
    private Number alignmentResultId;
    private AlignmentResultFiles alignmentResultFiles;
    private SampleAlignmentResult sampleAlignmentResult;

    public Number getAlignmentResultId() {
        return alignmentResultId;
    }

    public void setAlignmentResultId(Number alignmentResultId) {
        this.alignmentResultId = alignmentResultId;
    }

    public AlignmentResultFiles getAlignmentResultFiles() {
        return alignmentResultFiles;
    }

    public void setAlignmentResultFiles(AlignmentResultFiles alignmentResultFiles) {
        this.alignmentResultFiles = alignmentResultFiles;
    }

    public SampleAlignmentResult getSampleAlignmentResult() {
        return sampleAlignmentResult;
    }

    public void setSampleAlignmentResult(SampleAlignmentResult sampleAlignmentResult) {
        this.sampleAlignmentResult = sampleAlignmentResult;
    }
}
