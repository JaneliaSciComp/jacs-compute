package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.it.jacs.model.domain.sample.PipelineResult;
import org.janelia.jacs2.asyncservice.imageservices.tools.ChannelComponents;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class SampleAreaResult {
    private String anatomicalArea;
    private String objective;
    private String resultDir;
    private String mergeRelativeSubDir;
    private String groupRelativeSubDir;
    private String stitchInfoFile;
    private String stitchRelativeSubDir;
    private String stichFile;
    private String mipsRelativeSubDir;
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

    public String getResultDir() {
        return resultDir;
    }

    public void setResultDir(String resultDir) {
        this.resultDir = resultDir;
    }

    public String getMergeRelativeSubDir() {
        return mergeRelativeSubDir;
    }

    public void setMergeRelativeSubDir(String mergeRelativeSubDir) {
        this.mergeRelativeSubDir = mergeRelativeSubDir;
    }

    public String getGroupRelativeSubDir() {
        return groupRelativeSubDir;
    }

    public void setGroupRelativeSubDir(String groupRelativeSubDir) {
        this.groupRelativeSubDir = groupRelativeSubDir;
    }

    public String getStitchRelativeSubDir() {
        return stitchRelativeSubDir;
    }

    public void setStitchRelativeSubDir(String stitchRelativeSubDir) {
        this.stitchRelativeSubDir = stitchRelativeSubDir;
    }

    public String getMipsRelativeSubDir() {
        return mipsRelativeSubDir;
    }

    public void setMipsRelativeSubDir(String mipsRelativeSubDir) {
        this.mipsRelativeSubDir = mipsRelativeSubDir;
    }

    public String getStitchInfoFile() {
        return stitchInfoFile;
    }

    public void setStitchInfoFile(String stitchInfoFile) {
        this.stitchInfoFile = stitchInfoFile;
    }

    public String getStichFile() {
        return stichFile;
    }

    public void setStichFile(String stichFile) {
        this.stichFile = stichFile;
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

    @JsonIgnore
    public List<String> getMergeResultFiles() {
        if (CollectionUtils.isNotEmpty(groupResults)) {
            return groupResults.stream().map(MergeTilePairResult::getMergeResultFile).filter(StringUtils::isNotBlank).collect(Collectors.toList());
        } else if (CollectionUtils.isNotEmpty(mergeResults)) {
            return mergeResults.stream().map(MergeTilePairResult::getMergeResultFile).filter(StringUtils::isNotBlank).collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }
}
