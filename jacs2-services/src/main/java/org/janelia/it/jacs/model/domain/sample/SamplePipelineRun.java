package org.janelia.it.jacs.model.domain.sample;

import org.janelia.it.jacs.model.domain.IndexedReference;
import org.janelia.it.jacs.model.domain.interfaces.HasIdentifier;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * A single service or pipeline run.
 */
public class SamplePipelineRun implements HasIdentifier {

    private Number id;
    private String name;
    private String pipelineProcess;
    private Integer pipelineVersion;
    private Date creationDate = new Date();
    private List<PipelineResult> results = new ArrayList<>();
    private PipelineError error;

    @Override
    public Number getId() {
        return id;
    }

    @Override
    public void setId(Number id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPipelineProcess() {
        return pipelineProcess;
    }

    public void setPipelineProcess(String pipelineProcess) {
        this.pipelineProcess = pipelineProcess;
    }

    public Integer getPipelineVersion() {
        return pipelineVersion;
    }

    public void setPipelineVersion(Integer pipelineVersion) {
        this.pipelineVersion = pipelineVersion;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    public List<PipelineResult> getResults() {
        return results;
    }

    public void setResults(List<PipelineResult> results) {
        this.results = results;
    }

    public void addResult(PipelineResult result) {
        results.add(result);
    }

    public PipelineError getError() {
        return error;
    }

    public void setError(PipelineError error) {
        this.error = error;
    }

    @SuppressWarnings("unchecked")
    public Stream<IndexedReference<PipelineResult, IndexedReference<Integer, Integer>[]>> streamResults() {
        IndexedReference<Integer, Integer>[] emptyTrace = (IndexedReference<Integer, Integer>[]) new IndexedReference[0];
        return IntStream.range(0, results.size())
                .mapToObj(pos -> new IndexedReference<>(results.get(pos), pos))
                .flatMap(indexedResult -> indexedResult.getReference().streamResults(0, indexedResult.getPos(), emptyTrace));
    }
}
