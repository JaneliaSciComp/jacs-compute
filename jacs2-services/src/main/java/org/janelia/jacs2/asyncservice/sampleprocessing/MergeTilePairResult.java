package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import org.janelia.jacs2.asyncservice.imageservices.tools.ChannelComponents;

import java.util.List;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class MergeTilePairResult {
    private String tileName;
    private String mergeResultFile;
    private String channelMapping;
    private String imageSize;
    private String opticalResolution;
    private ChannelComponents channelComponents;
    private List<String> channelColors;

    public String getTileName() {
        return tileName;
    }

    public void setTileName(String tileName) {
        this.tileName = tileName;
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

    public ChannelComponents getChannelComponents() {
        return channelComponents;
    }

    public void setChannelComponents(ChannelComponents channelComponents) {
        this.channelComponents = channelComponents;
    }

    public List<String> getChannelColors() {
        return channelColors;
    }

    public void setChannelColors(List<String> channelColors) {
        this.channelColors = channelColors;
    }
}
