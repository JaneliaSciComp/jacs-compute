package org.janelia.jacs2.dataservice.sample;

import org.janelia.jacs2.dao.SageDao;
import org.janelia.jacs2.model.page.PageRequest;
import org.janelia.jacs2.model.sage.SlideImage;

import javax.inject.Inject;
import java.util.List;

public class SageDataService {

    private final SageDao sageDao;

    @Inject
    public SageDataService(SageDao sageDao) {
        this.sageDao = sageDao;
    }

    public List<SlideImage> getSlideImagesByDatasetAndLsmNames(String dataset, List<String> lsmNames, PageRequest pageRequest) {
        return sageDao.findSlideImagesByDatasetAndLsmNames(dataset, lsmNames, (int) pageRequest.getOffset(), pageRequest.getPageSize());
    }
}
