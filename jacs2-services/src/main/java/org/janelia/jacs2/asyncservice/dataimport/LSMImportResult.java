package org.janelia.jacs2.asyncservice.dataimport;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LSMImportResult {
    final String dataset;
    final Number sampleId;
    final String sampleName;
    final boolean newSample;

    @JsonCreator
    public LSMImportResult(@JsonProperty("dataset") String dataset,
                           @JsonProperty("sampleId") Number sampleId,
                           @JsonProperty("sampleName") String sampleName,
                           @JsonProperty("newSample") boolean newSample) {
        this.dataset = dataset;
        this.sampleId = sampleId;
        this.sampleName = sampleName;
        this.newSample = newSample;
    }

    public String getDataset() {
        return dataset;
    }

    public Number getSampleId() {
        return sampleId;
    }

    public String getSampleName() {
        return sampleName;
    }

    public boolean isNewSample() {
        return newSample;
    }

}
