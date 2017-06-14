package org.janelia.it.jacs.model.domain.sample;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * Summary files for all of the LSMs in an ObjectiveSample.
 * Generally this consists of MIPs and movies.
 */
public class LSMSummaryResult extends PipelineResult {

    private List<FileGroup> groups = new ArrayList<>();

    public List<FileGroup> getGroups() {
        return groups;
    }

    public void setGroups(List<FileGroup> groups) {
        Preconditions.checkArgument(groups != null, "Groups cannot be null");
        this.groups = groups;
    }

}
