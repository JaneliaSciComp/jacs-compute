package org.janelia.model.access.dao;

/**
 * Read only archived data access spec.
 *
 * @param <T> entity type
 * @param <I> entity identifier type
 */
public interface ArchiveReadDao<T, I> {
    T findArchivedEntityById(I id);
}
