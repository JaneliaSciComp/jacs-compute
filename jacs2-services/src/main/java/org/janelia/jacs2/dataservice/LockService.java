package org.janelia.jacs2.dataservice;

import org.janelia.model.jacs2.domain.DomainObject;

import java.util.concurrent.TimeUnit;

public interface LockService {
    <T extends DomainObject> String lock(T entity, long timeout, TimeUnit timeunit);
    <T extends DomainObject> String tryLock(T entity);
    <T extends DomainObject> boolean isLocked(T entity);
    <T extends DomainObject> boolean unlock(String lockKey, T entity);
}
