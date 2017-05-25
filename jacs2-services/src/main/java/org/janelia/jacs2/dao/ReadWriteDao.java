package org.janelia.jacs2.dao;

import java.util.List;
import java.util.Map;

/**
 * Read/Write data access spec.
 *
 * @param <T> entity type
 * @param <I> entity identifier type
 */
public interface ReadWriteDao<T, I> extends ReadOnlyDao<T, I> {
    void save(T entity);
    void saveAll(List<T> entities);
    void update(T entity, Map<String, Object> fieldsToUpdate);
    void delete(T entity);
    void archive(T entity);
}
