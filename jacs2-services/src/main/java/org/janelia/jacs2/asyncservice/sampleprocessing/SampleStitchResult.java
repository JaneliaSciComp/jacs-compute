package org.janelia.jacs2.asyncservice.sampleprocessing;

import org.janelia.it.jacs.model.domain.sample.PipelineResult;

public class SampleStitchResult extends PipelineResult {
    private String anatomicalArea;
    private String imageSize;
    private String opticalResolution;
    private String channelColors;
    private String channelSpec;

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

    public void setChannelSpec(String channelSpec) {
        this.channelSpec = channelSpec;
    }
}
