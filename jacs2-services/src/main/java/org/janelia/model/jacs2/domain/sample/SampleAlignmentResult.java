package org.janelia.model.jacs2.domain.sample;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.janelia.model.jacs2.domain.enums.AlignmentScoreType;

import java.util.HashMap;
import java.util.Map;

/**
 * The result of running an alignment algorithm on a sample. 
 */
public class SampleAlignmentResult extends PipelineResult {

	private String anatomicalArea;
    private String imageSize;
    private String opticalResolution;
    private String channelColors;
    private String channelSpec;
    private String objective;
    private String alignmentSpace;
    private String boundingBox;
    private Map<AlignmentScoreType, String> scores = new HashMap<>();

    public String getAnatomicalArea() {
        return anatomicalArea;
    }

    public void setAnatomicalArea(String anatomicalArea) {
        this.anatomicalArea = anatomicalArea;
    }
    
    public String getImageSize() {
        return imageSize;
    }

    public void setImageSize(String imageSize) {
        this.imageSize = imageSize;
    }

    public String getOpticalResolution() {
        return opticalResolution;
    }

    public void setOpticalResolution(String opticalResolution) {
        this.opticalResolution = opticalResolution;
    }

    public String getChannelColors() {
        return channelColors;
    }

    public void setChannelColors(String channelColors) {
        this.channelColors = channelColors;
    }

    public String getChannelSpec() {
        return channelSpec;
    }

    public void setChannelSpec(String chanSpec) {
        this.channelSpec = chanSpec;
    }

    public String getObjective() {
        return objective;
    }

    public void setObjective(String objective) {
        this.objective = objective;
    }
    
    public String getAlignmentSpace() {
        return alignmentSpace;
    }

    public void setAlignmentSpace(String alignmentSpace) {
        this.alignmentSpace = alignmentSpace;
    }

    public String getBoundingBox() {
        return boundingBox;
    }

    public void setBoundingBox(String boundingBox) {
        this.boundingBox = boundingBox;
    }

    public Map<AlignmentScoreType, String> getScores() {
        return scores;
    }

    public void setScores(Map<AlignmentScoreType, String> scores) {
        Preconditions.checkArgument(scores != null, "Scores cannot be null");
        this.scores = scores;
    }

    public void addScore(AlignmentScoreType scoreType, String score) {
        if (StringUtils.isNotBlank(score)) scores.put(scoreType, score);
    }

    public void addScores(Map<AlignmentScoreType, String> scores) {
        scores.entrySet().stream().forEach(entry -> addScore(entry.getKey(), entry.getValue()));
    }
}
