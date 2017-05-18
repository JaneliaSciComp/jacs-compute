package org.janelia.jacs2.asyncservice.imageservices.stitching;

import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class StitchedImageInfo {
    private String thumbnailFile;
    private int numberOfTiles;
    private String dimensions;
    private String origin;
    private String resolution;
    private List<String> tileCoordinates;
    private String mstLut;

    public String getThumbnailFile() {
        return thumbnailFile;
    }

    public void setThumbnailFile(String thumbnailFile) {
        this.thumbnailFile = thumbnailFile;
    }

    public int getNumberOfTiles() {
        return numberOfTiles;
    }

    public void setNumberOfTiles(int numberOfTiles) {
        this.numberOfTiles = numberOfTiles;
    }

    public String getDimensions() {
        return dimensions;
    }

    public void setDimensions(String dimensions) {
        this.dimensions = dimensions;
    }

    public String getXYZDimensions() {
        if (StringUtils.isBlank(dimensions)) return null;
        List<String> parts = Splitter.on(' ').omitEmptyStrings().splitToList(dimensions);
        if (parts.size() < 3) return null;
        return parts.get(0)+"x"+parts.get(1)+"x"+parts.get(2);
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public String getResolution() {
        return resolution;
    }

    public void setResolution(String resolution) {
        this.resolution = resolution;
    }

    public List<String> getTileCoordinates() {
        return tileCoordinates;
    }

    public void setTileCoordinates(List<String> tileCoordinates) {
        this.tileCoordinates = tileCoordinates;
    }

    public String getMstLut() {
        return mstLut;
    }

    public void setMstLut(String mstLut) {
        this.mstLut = mstLut;
    }
}
