package org.janelia.jacs2.asyncservice.common;

import org.janelia.jacs2.asyncservice.sampleprocessing.SampleProcessorResult;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;

import java.util.List;

public final class JacsServiceResult<T> {
    private final JacsServiceData jacsServiceData;
    private T result;
    private List<SampleProcessorResult> sampleProcessorResults;

    public JacsServiceResult(JacsServiceData jacsServiceData) {
        this.jacsServiceData = jacsServiceData;
    }

    public JacsServiceResult(JacsServiceData jacsServiceData, T result) {
        this.jacsServiceData = jacsServiceData;
        this.result = result;
    }

    public final JacsServiceData getJacsServiceData() {
        return jacsServiceData;
    }

    public final T getResult() {
        return result;
    }

    public final void setResult(T result) {
        this.result = result;
    }

    public void setSampleProcessorResults(List<SampleProcessorResult> sampleProcessorResults) {
        this.sampleProcessorResults = sampleProcessorResults;
    }

    public List<SampleProcessorResult> getSampleProcessorResults() {
        return sampleProcessorResults;
    }
}
