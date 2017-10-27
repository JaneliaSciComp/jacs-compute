package org.janelia.model.jacs2.dao;

import org.janelia.model.access.dao.ReadWriteDao;
import org.janelia.model.jacs2.domain.DomainObject;
import org.janelia.model.jacs2.domain.Subject;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;

import java.util.List;

public interface DomainObjectDao<T extends DomainObject> extends ReadWriteDao<T, Number> {
    List<T> findByIds(Subject subject, List<Number> ids);
    <U extends T> List<U> findSubtypesByIds(Subject subject, List<Number> ids, Class<U> entityType);
    PageResult<T> findByOwnerKey(Subject subject, String ownerKey, PageRequest pageRequest);
    boolean lockEntity(String lockKey, T entity);
    boolean unlockEntity(String lockKey, T entity);
}
