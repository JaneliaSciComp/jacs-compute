package org.janelia.it.jacs.model.domain.sample;

import org.janelia.it.jacs.model.domain.ReverseReference;

/**
 * The result of running the Neuron Separator on some input file.
 */
public class NeuronSeparation extends PipelineResult {

    private ReverseReference fragments;

    private Boolean hasWeights = false;

    public ReverseReference getFragmentsReference() {
        return fragments;
    }

    public void setFragmentsReference(ReverseReference fragmentsReference) {
        this.fragments = fragmentsReference;
    }

    public Boolean getHasWeights() {
        return hasWeights;
    }

    public void setHasWeights(Boolean hasWeights) {
        this.hasWeights = hasWeights;
    }
}
