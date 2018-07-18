package org.janelia.jacs2.asyncservice.imageservices;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class FileConverterResult {
    String inputFileName;
    String outputFileName;

    @JsonCreator
    public FileConverterResult(@JsonProperty("inputFileName") String inputFileName, @JsonProperty("outputFileName") String outputFileName) {
        this.inputFileName = inputFileName;
        this.outputFileName = outputFileName;
    }

    public String getInputFileName() {
        return inputFileName;
    }

    public String getOutputFileName() {
        return outputFileName;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("inputFileName", inputFileName)
                .append("outputFileName", outputFileName)
                .toString();
    }
}
