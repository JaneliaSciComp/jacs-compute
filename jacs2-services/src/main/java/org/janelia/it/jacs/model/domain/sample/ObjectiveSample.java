package org.janelia.it.jacs.model.domain.sample;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import org.janelia.it.jacs.model.domain.IndexedReference;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

/**
 * A set of LSMs in a Sample with a common objective.
 */
public class ObjectiveSample {

    private String objective;
    private String chanSpec;
    private List<SampleTile> tiles = new ArrayList<>();
    private List<SamplePipelineRun> pipelineRuns;

    @JsonIgnore
    private transient Sample parent;

    public String getObjective() {
        return objective;
    }

    public void setObjective(String objective) {
        this.objective = objective;
    }

    public String getChanSpec() {
        return chanSpec;
    }

    public void setChanSpec(String chanSpec) {
        this.chanSpec = chanSpec;
    }

    public List<SampleTile> getTiles() {
        return tiles;
    }

    public void setTiles(List<SampleTile> tiles) {
        Preconditions.checkArgument(tiles != null, "The tile list for a sample objective cannot be null");
        this.tiles = tiles;
    }

    public void addTiles(SampleTile... tiles) {
        for (SampleTile t : tiles) {
            t.setParent(this);
            this.tiles.add(t);
        }
    }

    public Sample getParent() {
        return parent;
    }

    public void setParent(Sample parent) {
        this.parent = parent;
    }

    public List<SamplePipelineRun> getPipelineRuns() {
        return pipelineRuns;
    }

    public void setPipelineRuns(List<SamplePipelineRun> pipelineRuns) {
        this.pipelineRuns = pipelineRuns;
    }

    public void addPipelineRun(SamplePipelineRun pipelineRun) {
        if (pipelineRuns == null) pipelineRuns = new ArrayList<>();
        pipelineRuns.add(pipelineRun);
    }

    public Optional<IndexedReference<SamplePipelineRun>> findPipelineRunById(Number runId) {
        if (pipelineRuns == null || runId == null) {
            return Optional.empty();
        } else {
            return IntStream.range(0, pipelineRuns.size())
                    .mapToObj(pos -> new IndexedReference<>(pipelineRuns.get(pos), pos))
                    .filter(positionalReference -> runId.toString().equals(positionalReference.getReference().getId().toString()))
                    .findFirst();
        }
    }

}
