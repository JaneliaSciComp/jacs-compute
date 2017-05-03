package org.janelia.jacs2.dao;

import org.janelia.jacs2.model.sage.SlideImage;

import java.util.List;

public interface SageDao extends ReadOnlyDao<SlideImage, Integer> {
    List<SlideImage> findSlideImagesByDatasetAndLsmNames(String dataset, List<String> lsmNames);
}
