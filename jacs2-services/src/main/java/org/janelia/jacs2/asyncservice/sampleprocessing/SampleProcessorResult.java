package org.janelia.jacs2.asyncservice.sampleprocessing;

class SampleProcessorResult {
    private Number sampleId;
    private String objective;
    private String area;
    private Number runId;
    private Number resultId;
    private String areaFile;
    private String signalChannels;
    private String referenceChannel;
    private String resultDir;

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
}
