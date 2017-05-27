package org.janelia.jacs2.asyncservice.sampleprocessing;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LSMSummary {
    private SampleImageFile sampleImageFile;
    private String mipsResultsDir;
    private List<String> mips = new ArrayList<>();
    private Map<String, String> montageResultsByType;

    public SampleImageFile getSampleImageFile() {
        return sampleImageFile;
    }

    public void setSampleImageFile(SampleImageFile sampleImageFile) {
        this.sampleImageFile = sampleImageFile;
    }

    public String getMipsResultsDir() {
        return mipsResultsDir;
    }

    public void setMipsResultsDir(String mipsResultsDir) {
        this.mipsResultsDir = mipsResultsDir;
    }

    public List<String> getMips() {
        return mips;
    }

    public void setMips(List<String> mips) {
        this.mips = mips;
    }

    public Map<String, String> getMontageResultsByType() {
        return montageResultsByType;
    }

    public void setMontageResultsByType(Map<String, String> montageResultsByType) {
        this.montageResultsByType = montageResultsByType;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
