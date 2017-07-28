package org.janelia.jacs2.model.sage;

import org.janelia.it.jacs.model.domain.sample.LSMImage;

import java.util.ArrayList;
import java.util.List;

/**
 * SlideImageGroup groups a list of lsms by the anatomical area.
 */
public class SlideImageGroup {

    private final String tag;
    private final List<LSMImage> images = new ArrayList<>();
    private String anatomicalArea;

    public SlideImageGroup(String tag, String anatomicalArea) {
        this.tag = tag;
        this.anatomicalArea = anatomicalArea;
    }

    public String getTag() {
        return tag;
    }

    public List<LSMImage> getImages() {
        return images;
    }

    public String getAnatomicalArea() {
        return anatomicalArea;
    }

    public void setAnatomicalArea(String anatomicalArea) {
        this.anatomicalArea = anatomicalArea;
    }

    public void addImage(LSMImage lsmImage) {
        images.add(lsmImage);
    }
}
