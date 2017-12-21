package org.janelia.model.access.dao;

/**
 * Archiver spec.
 *
 * @param <T> entity type
 */
public interface ArchiveWriteDao<T> {
    void archive(T entity);
}
