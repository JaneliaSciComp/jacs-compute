package org.janelia.jacs2.asyncservice.sampleprocessing;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class SampleProcessorResult {
    private Number sampleId;
    private String objective;
    private String area;
    private Number runId;
    private Number resultId;
    private String areaFile;
    private String signalChannels;
    private String referenceChannel;
    private String resultDir;
    private String imageSize;
    private String opticalResolution;
    private String referenceChannelNumber;
    private String chanSpec;
    private int numChannels;

    public Number getSampleId() {
        return sampleId;
    }

    public void setSampleId(Number sampleId) {
        this.sampleId = sampleId;
    }

    public String getObjective() {
        return objective;
    }

    public void setObjective(String objective) {
        this.objective = objective;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public Number getRunId() {
        return runId;
    }

    public void setRunId(Number runId) {
        this.runId = runId;
    }

    public Number getResultId() {
        return resultId;
    }

    public void setResultId(Number resultId) {
        this.resultId = resultId;
    }

    public String getAreaFile() {
        return areaFile;
    }

    public void setAreaFile(String areaFile) {
        this.areaFile = areaFile;
    }

    public String getSignalChannels() {
        return signalChannels;
    }

    public void setSignalChannels(String signalChannels) {
        this.signalChannels = signalChannels;
    }

    public String getReferenceChannel() {
        return referenceChannel;
    }

    public void setReferenceChannel(String referenceChannel) {
        this.referenceChannel = referenceChannel;
    }

    public String getResultDir() {
        return resultDir;
    }

    public void setResultDir(String resultDir) {
        this.resultDir = resultDir;
    }

    public String getImageSize() {
        return imageSize;
    }

    public void setImageSize(String imageSize) {
        this.imageSize = imageSize;
    }

    public String getOpticalResolution() {
        return opticalResolution;
    }

    public void setOpticalResolution(String opticalResolution) {
        this.opticalResolution = opticalResolution;
    }

    public String getReferenceChannelNumber() {
        return referenceChannelNumber;
    }

    public void setReferenceChannelNumber(String referenceChannelNumber) {
        this.referenceChannelNumber = referenceChannelNumber;
    }

    public String getChanSpec() {
        return chanSpec;
    }

    public void setChanSpec(String chanSpec) {
        this.chanSpec = chanSpec;
    }

    public int getNumChannels() {
        return numChannels;
    }

    public void setNumChannels(int numChannels) {
        this.numChannels = numChannels;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("sampleId", sampleId)
                .append("objective", objective)
                .append("area", area)
                .append("runId", runId)
                .append("resultId", resultId)
                .append("areaFile", areaFile)
                .append("chanSpec", chanSpec)
                .build();
    }
}
