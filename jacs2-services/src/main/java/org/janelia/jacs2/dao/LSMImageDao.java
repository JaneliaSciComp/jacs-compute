package org.janelia.jacs2.dao;

import org.janelia.it.jacs.model.domain.Subject;
import org.janelia.it.jacs.model.domain.sample.LSMImage;
import org.janelia.jacs2.model.page.PageRequest;
import org.janelia.jacs2.model.page.PageResult;

public interface LSMImageDao extends ImageDao<LSMImage> {
    PageResult<LSMImage> findMatchingLSMs(Subject subject, LSMImage pattern, PageRequest pageRequest);
}
