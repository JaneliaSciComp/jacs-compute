package org.janelia.jacs2.dao;

import org.janelia.jacs2.model.sage.ControlledVocabulary;
import org.janelia.jacs2.model.sage.SlideImage;

import java.util.List;

public interface SageDao extends Dao<SlideImage, Integer> {
    List<ControlledVocabulary> findAllUsedLineVocabularyTerms(List<String> vocabularyNames);
    List<ControlledVocabulary> findAllUsedDatasetVocabularyTerms (String dataset, List<String> lsmNames);
    List<SlideImage> findSlideImagesByDatasetAndLsmNames(String dataset, List<String> lsmNames, int offset, int length);
}
