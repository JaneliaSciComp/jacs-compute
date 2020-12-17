package org.janelia.jacs2.asyncservice.imagesearch;

import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class CDMaskMatches {
    private String maskId;
    private List<CDSMatchResult> results;

    public String getMaskId() {
        return maskId;
    }

    public CDMaskMatches setMaskId(String maskId) {
        this.maskId = maskId;
        return this;
    }

    boolean hasResults() {
        return results != null && !results.isEmpty();
    }

    public List<CDSMatchResult> getResults() {
        return results;
    }

    public void setResults(List<CDSMatchResult> results) {
        this.results = results;
    }

    public CDMaskMatches addResults(List<CDSMatchResult> results) {
        if (!hasResults()) {
            this.results = results;
        } else if (results != null && !results.isEmpty()) {
            this.results.addAll(results);
        }
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("maskId", maskId)
                .toString();
    }
}
