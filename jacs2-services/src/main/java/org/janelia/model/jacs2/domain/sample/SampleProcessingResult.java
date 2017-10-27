package org.janelia.model.jacs2.domain.sample;

/**
 * The result of processing the LSMs of a single anatomical area of an ObjectiveSample.
 */
public class SampleProcessingResult extends PipelineResult {

    private String anatomicalArea;
    private String imageSize;
    private String opticalResolution;
    private String channelColors;
    private String chanelSpec;

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
        return chanelSpec;
    }

    public void setChannelSpec(String chanSpec) {
        this.chanelSpec = chanSpec;
    }
}
