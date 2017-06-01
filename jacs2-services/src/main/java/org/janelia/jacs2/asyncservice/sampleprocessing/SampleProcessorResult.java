package org.janelia.jacs2.asyncservice.sampleprocessing;

class SampleProcessorResult {
    private Number sampleId;
    private String objective;
    private String area;
    private Number runId;
    private String tileFile;

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

    public String getTileFile() {
        return tileFile;
    }

    public void setTileFile(String tileFile) {
        this.tileFile = tileFile;
    }
}
