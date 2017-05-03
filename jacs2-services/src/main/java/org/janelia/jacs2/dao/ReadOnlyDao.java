package org.janelia.jacs2.dao;

import org.janelia.jacs2.model.page.PageRequest;
import org.janelia.jacs2.model.page.PageResult;

/**
 * Read only data access spec.
 *
 * @param <T> entity type
 * @param <I> entity identifier type
 */
public interface ReadOnlyDao<T, I> extends Dao<T, I> {
    T findById(I id);
    PageResult<T> findAll(PageRequest pageRequest);
    long countAll();
}
