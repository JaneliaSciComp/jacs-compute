package org.janelia.jacs2.asyncservice.sampleprocessing;

import org.janelia.jacs2.asyncservice.alignservices.AlignmentResultFiles;

public class AlignmentResult {
    private Number alignmentResultId;
    private AlignmentResultFiles alignmentResultFiles;

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
}
