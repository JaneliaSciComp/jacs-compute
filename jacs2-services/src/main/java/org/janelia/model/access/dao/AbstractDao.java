package org.janelia.model.access.dao;

import org.janelia.model.jacs2.DomainModelUtils;

public abstract class AbstractDao<T, I> implements Dao<T, I> {
    protected Class<T> getEntityType() {
        return DomainModelUtils.getGenericParameterType(this.getClass(), 0);
    }
}
