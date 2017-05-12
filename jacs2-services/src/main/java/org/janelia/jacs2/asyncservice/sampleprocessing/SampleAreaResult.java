package org.janelia.jacs2.asyncservice.sampleprocessing;

import org.janelia.jacs2.asyncservice.imageservices.tools.ChannelComponents;

import java.util.ArrayList;
import java.util.List;

public class SampleAreaResult {
    private String anatomicalArea;
    private String objective;
    private String mergeDir;
    private String groupDir;
    private String stitchInfoFile;
    private String stitchDir;
    private String stichFile;
    private String mipsDir;
    private List<String> mipsFileList = new ArrayList<>();
    private String consensusChannelMapping;
    private ChannelComponents consensusChannelComponents;
    private List<MergeTilePairResult> mergeResults;
    private List<MergeTilePairResult> groupResults;

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

    public String getMergeDir() {
        return mergeDir;
    }

    public void setMergeDir(String mergeDir) {
        this.mergeDir = mergeDir;
    }

    public String getGroupDir() {
        return groupDir;
    }

    public void setGroupDir(String groupDir) {
        this.groupDir = groupDir;
    }

    public String getStitchInfoFile() {
        return stitchInfoFile;
    }

    public void setStitchInfoFile(String stitchInfoFile) {
        this.stitchInfoFile = stitchInfoFile;
    }

    public String getStitchDir() {
        return stitchDir;
    }

    public void setStitchDir(String stitchDir) {
        this.stitchDir = stitchDir;
    }

    public String getStichFile() {
        return stichFile;
    }

    public void setStichFile(String stichFile) {
        this.stichFile = stichFile;
    }

    public String getMipsDir() {
        return mipsDir;
    }

    public void setMipsDir(String mipsDir) {
        this.mipsDir = mipsDir;
    }

    public List<String> getMipsFileList() {
        return mipsFileList;
    }

    public void setMipsFileList(List<String> mipsFileList) {
        this.mipsFileList = mipsFileList;
    }

    public void addMips(List<String> mipsFileList) {
        this.mipsFileList.addAll(mipsFileList);
    }

    public String getConsensusChannelMapping() {
        return consensusChannelMapping;
    }

    public void setConsensusChannelMapping(String consensusChannelMapping) {
        this.consensusChannelMapping = consensusChannelMapping;
    }

    public ChannelComponents getConsensusChannelComponents() {
        return consensusChannelComponents;
    }

    public void setConsensusChannelComponents(ChannelComponents consensusChannelComponents) {
        this.consensusChannelComponents = consensusChannelComponents;
    }

    public List<MergeTilePairResult> getMergeResults() {
        return mergeResults;
    }

    public void setMergeResults(List<MergeTilePairResult> mergeResults) {
        this.mergeResults = mergeResults;
    }

    public List<MergeTilePairResult> getGroupResults() {
        return groupResults;
    }

    public void setGroupResults(List<MergeTilePairResult> groupResults) {
        this.groupResults = groupResults;
    }
}
