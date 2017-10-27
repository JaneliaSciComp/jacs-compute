package org.janelia.model.jacs2.dao;

import org.janelia.model.jacs2.domain.Subject;
import org.janelia.model.jacs2.domain.sample.LSMImage;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;

public interface LSMImageDao extends ImageDao<LSMImage> {
    PageResult<LSMImage> findMatchingLSMs(Subject subject, LSMImage pattern, PageRequest pageRequest);
}
