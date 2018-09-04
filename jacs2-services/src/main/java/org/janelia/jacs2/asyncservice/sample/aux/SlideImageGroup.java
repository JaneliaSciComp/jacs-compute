package org.janelia.jacs2.asyncservice.sample.aux;

import java.util.ArrayList;
import java.util.List;

import org.janelia.model.domain.sample.LSMImage;

/**
 * A group of SlideImages with a tag and an anatomical area. In general, a TileImageGroup usually contains either
 * a single LSM, or two LSMs to be merged, but it may contain an arbitrary number of images.  
 * 
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SlideImageGroup {

    private final String tag;
    private final List<LSMImage> images = new ArrayList<>();
    private String anatomicalArea;

    public SlideImageGroup(String anatomicalArea, String tag) {
        this.tag = tag;
    	setAnatomicalArea(anatomicalArea);
    }
    
    public String getAnatomicalArea() {
        return anatomicalArea;
    }
    
    public void setAnatomicalArea(String anatomicalArea) {
		this.anatomicalArea = anatomicalArea==null?"":anatomicalArea;
	}

	public String getTag() {
		return tag;
	}

	public List<LSMImage> getImages() {
		return images;
	}

	public void addFile(LSMImage image) {
		images.add(image);
	}
}