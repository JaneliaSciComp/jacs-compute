package org.janelia.jacs2.dataservice.persistence;

import org.apache.commons.collections4.CollectionUtils;
import org.janelia.model.access.dao.ReadWriteDao;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;

import javax.enterprise.inject.Instance;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class AbstractDataPersistence<D extends ReadWriteDao<T, I>, T, I> {
    Instance<D> daoSource;

    AbstractDataPersistence(Instance<D> daoSource) {
        this.daoSource = daoSource;
    }

    public T createEntity(T entity) {
        D dao = daoSource.get();
        try {
            dao.save(entity);
            return entity;
        } finally {
            daoSource.destroy(dao);
        }
    }

    public T findById(I id) {
        D dao = daoSource.get();
        try {
            return findById(dao, id);
        } finally {
            daoSource.destroy(dao);
        }
    }

    protected T findById(D dao, I id) {
        return dao.findById(id);
    }

    public List<T> findByIds(Collection<I> ids) {
        D dao = daoSource.get();
        try {
            return dao.findByIds(ids);
        } finally {
            daoSource.destroy(dao);
        }
    }

    public PageResult<T> findAll(PageRequest pageRequest) {
        D dao = daoSource.get();
        try {
            return dao.findAll(pageRequest);
        } finally {
            daoSource.destroy(dao);
        }
    }

    public void save(T t) {
        D dao = daoSource.get();
        try {
            dao.save(t);
        } finally {
            daoSource.destroy(dao);
        }
    }

    public void update(T t, Map<String, EntityFieldValueHandler<?>> fieldsToUpdate) {
        if (fieldsToUpdate != null && fieldsToUpdate.size() > 0) {
            D dao = daoSource.get();
            try {
                dao.update(t, fieldsToUpdate);
            } finally {
                daoSource.destroy(dao);
            }
        }
    }
}
