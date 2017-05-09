package org.janelia.jacs2.dao;

import org.janelia.jacs2.model.sage.SlideImage;

import java.util.List;

public interface SageDao extends Dao<SlideImage, Integer> {
    List<SlideImage> findMatchingSlideImages(String dataset, String line, List<String> slideCodes, List<String> lsmNames, int offset, int length);
}
