package org.janelia.jacs2.dataservice.sample;

import org.janelia.model.jacs2.dao.SageDao;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.sage.SlideImage;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import java.util.List;

@Dependent
public class SageDataService {

    private final SageDao sageDao;

    @Inject
    public SageDataService(SageDao sageDao) {
        this.sageDao = sageDao;
    }

    public List<SlideImage> getMatchingImages(String dataset, String line, List<String> slideCodes, List<String> lsmNames, PageRequest pageRequest) {
        return sageDao.findMatchingSlideImages(dataset, line, slideCodes, lsmNames, (int) pageRequest.getOffset(), pageRequest.getPageSize());
    }

}
