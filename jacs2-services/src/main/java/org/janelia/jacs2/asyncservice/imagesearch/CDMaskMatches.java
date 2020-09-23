package org.janelia.jacs2.asyncservice.imagesearch;

import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class CDMaskMatches {
    private String maskId;
    private List<CDSMatchResult> results;

    public String getMaskId() {
        return maskId;
    }

    public void setMaskId(String maskId) {
        this.maskId = maskId;
    }

    public List<CDSMatchResult> getResults() {
        return results;
    }

    public void setResults(List<CDSMatchResult> results) {
        this.results = results;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("maskId", maskId)
                .toString();
    }
}
