package org.janelia.model.access.dao;

/**
 * Archived read/write access spec.
 *
 * @param <T> entity type
 * @param <I> entity identifier type
 */
public interface ArchiveDao<T, I> extends ArchiveReadDao<T, I>, ArchiveWriteDao<T>  {
}
