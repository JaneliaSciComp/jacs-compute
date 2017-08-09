package org.janelia.jacs2.asyncservice.dataimport;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class LSMImportResult {
    final String dataset;
    final Number sampleId;
    final String sampleName;
    final String line;
    final String slideCode;
    final List<String> imageNames;
    final boolean newSample;

    @JsonCreator
    public LSMImportResult(@JsonProperty("dataset") String dataset,
                           @JsonProperty("sampleId") Number sampleId,
                           @JsonProperty("sampleName") String sampleName,
                           @JsonProperty("line") String line,
                           @JsonProperty("slideCode") String slideCode,
                           @JsonProperty("imageNames") List<String> imageNames,
                           @JsonProperty("newSample") boolean newSample) {
        this.dataset = dataset;
        this.sampleId = sampleId;
        this.sampleName = sampleName;
        this.line = line;
        this.slideCode = slideCode;
        this.imageNames = imageNames;
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

    public String getLine() {
        return line;
    }

    public String getSlideCode() {
        return slideCode;
    }

    public List<String> getImageNames() {
        return imageNames;
    }

    public boolean isNewSample() {
        return newSample;
    }

}
