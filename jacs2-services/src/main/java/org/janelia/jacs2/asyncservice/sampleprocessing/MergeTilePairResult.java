package org.janelia.jacs2.asyncservice.sampleprocessing;

import org.janelia.jacs2.asyncservice.imageservices.tools.ChannelComponents;

public class MergeTilePairResult {
    private String anatomicalArea;
    private String objective;
    private String tileName;
    private String mergeResultDir;
    private String mergeResultFile;
    private String channelMapping;
    private ChannelComponents channelComponents;

    public String getAnatomicalArea() {
        return anatomicalArea;
    }

    public void setAnatomicalArea(String anatomicalArea) {
        this.anatomicalArea = anatomicalArea;
    }

    public String getObjective() {
        return objective;
    }

    public void setObjective(String objective) {
        this.objective = objective;
    }

    public String getTileName() {
        return tileName;
    }

    public void setTileName(String tileName) {
        this.tileName = tileName;
    }

    public String getMergeResultDir() {
        return mergeResultDir;
    }

    public void setMergeResultDir(String mergeResultDir) {
        this.mergeResultDir = mergeResultDir;
    }

    public String getMergeResultFile() {
        return mergeResultFile;
    }

    public void setMergeResultFile(String mergeResultFile) {
        this.mergeResultFile = mergeResultFile;
    }

    public String getChannelMapping() {
        return channelMapping;
    }

    public void setChannelMapping(String channelMapping) {
        this.channelMapping = channelMapping;
    }

    public ChannelComponents getChannelComponents() {
        return channelComponents;
    }

    public void setChannelComponents(ChannelComponents channelComponents) {
        this.channelComponents = channelComponents;
    }
}
