package org.janelia.jacs2.asyncservice.spark;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.builder.ToStringBuilder;

class SparkClusterInfo {
    private final Long masterJobId;
    private final Long workerJobId;
    private final String masterURI;

    @JsonCreator
    SparkClusterInfo(@JsonProperty("masterJobId") Long masterJobId,
                     @JsonProperty("workerJobId") Long workerJobId,
                     @JsonProperty("masterURI") String masterURI) {
        this.masterJobId = masterJobId;
        this.workerJobId = workerJobId;
        this.masterURI = masterURI;
    }

    public Long getMasterJobId() {
        return masterJobId;
    }

    public Long getWorkerJobId() {
        return workerJobId;
    }

    public String getMasterURI() {
        return masterURI;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("masterJobId", masterJobId)
                .append("workerJobId", workerJobId)
                .append("masterURI", masterURI)
                .toString();
    }
}
