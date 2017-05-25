package org.janelia.jacs2.dataservice.persistence;

import org.janelia.jacs2.dao.ReadWriteDao;

import javax.enterprise.inject.Instance;
import java.util.Map;

public class AbstractDataPersistence<D extends ReadWriteDao<T, I>, T, I> {
    protected Instance<D> daoSource;

    AbstractDataPersistence(Instance<D> daoSource) {
        this.daoSource = daoSource;
    }

    public T findById(I id) {
        D dao = daoSource.get();
        try {
            return dao.findById(id);
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

    public void update(T t, Map<String, Object> fieldsToUpdate) {
        D dao = daoSource.get();
        try {
            dao.update(t, fieldsToUpdate);
        } finally {
            daoSource.destroy(dao);
        }
    }
}
