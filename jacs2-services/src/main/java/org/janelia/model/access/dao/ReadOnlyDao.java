package org.janelia.model.access.dao;

import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;

import java.util.Collection;
import java.util.List;

/**
 * Read only data access spec.
 *
 * @param <T> entity type
 * @param <I> entity identifier type
 */
public interface ReadOnlyDao<T, I> extends Dao<T, I> {
    T findById(I id);
    List<T> findByIds(Collection<I> ids);
    PageResult<T> findAll(PageRequest pageRequest);
    long countAll();
}
