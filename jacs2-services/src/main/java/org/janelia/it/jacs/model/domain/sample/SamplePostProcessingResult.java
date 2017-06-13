package org.janelia.it.jacs.model.domain.sample;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Post-processing that is run on the tile images and (if relevant) the stitched image.
 * <p>
 * This differs from the SampleProcessingResult in that it is not specific to a single
 * anatomical area. Thus, a single post-processing result may contain results from the
 * processing of multiple areas. For example, the Brain and VNC may be processed together
 * in order to normalize both.
 */
public class SamplePostProcessingResult extends PipelineResult {

    private List<FileGroup> groups = new ArrayList<>();

    @JsonIgnore
    public Set<String> getGroupKeys() {
        Set<String> groupKeys = new LinkedHashSet<>();
        for (FileGroup fileGroup : groups) {
            groupKeys.add(fileGroup.getKey());
        }
        return groupKeys;
    }

    @JsonIgnore
    public FileGroup getGroup(String key) {
        for (FileGroup fileGroup : groups) {
            if (fileGroup.getKey().equals(key)) {
                return fileGroup;
            }
        }
        return null;
    }

    public List<FileGroup> getGroups() {
        return groups;
    }

    public void setGroups(List<FileGroup> groups) {
        Preconditions.checkArgument(groups != null, "Groups cannot be null");
        this.groups = groups;
    }
}
