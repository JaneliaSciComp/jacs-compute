package org.janelia.jacs2.dao;

import org.janelia.jacs2.model.EntityFieldValueHandler;

import java.util.Collection;
import java.util.Map;

/**
 * Read/Write data access spec.
 *
 * @param <T> entity type
 * @param <I> entity identifier type
 */
public interface ReadWriteDao<T, I> extends ReadOnlyDao<T, I> {
    void save(T entity);
    void saveAll(Collection<T> entities);
    void update(T entity, Map<String, EntityFieldValueHandler<?>> fieldsToUpdate);
    void delete(T entity);
    void archive(T entity);
}
