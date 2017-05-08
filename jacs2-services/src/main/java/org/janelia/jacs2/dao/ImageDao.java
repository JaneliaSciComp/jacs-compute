package org.janelia.jacs2.dao;

import org.janelia.it.jacs.model.domain.sample.Image;

public interface ImageDao<T extends Image> extends DomainObjectDao<T> {
    void updateImageFiles(T image);
}
