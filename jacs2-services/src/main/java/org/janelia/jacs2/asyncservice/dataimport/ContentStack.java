package org.janelia.jacs2.asyncservice.dataimport;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.ArrayList;
import java.util.List;

@Deprecated
public class ContentStack {
    private Number dataNodeId;
    private final StorageContentInfo mainRep; // main content representation
    private List<StorageContentInfo> additionalReps = new ArrayList<>(); // additional representations

    @JsonCreator
    public ContentStack(@JsonProperty("mainRep") StorageContentInfo mainRep) {
        this.mainRep = mainRep;
    }

    public Number getDataNodeId() {
        return dataNodeId;
    }

    public void setDataNodeId(Number dataNodeId) {
        this.dataNodeId = dataNodeId;
    }

    public StorageContentInfo getMainRep() {
        return mainRep;
    }

    public List<StorageContentInfo> getAdditionalReps() {
        return additionalReps;
    }

    public void setAdditionalReps(List<StorageContentInfo> additionalReps) {
        this.additionalReps = additionalReps;
    }

    public void addRepresentation(StorageContentInfo contentInfo) {
        additionalReps.add(contentInfo);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("dataNodeId", dataNodeId)
                .append("mainRep", mainRep)
                .append("additionalReps", additionalReps)
                .toString();
    }
}
