package org.janelia.jacs2.dao;

import java.util.LinkedList;
import java.util.List;

import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.janelia.model.access.dao.Dao;
import org.janelia.model.jacs2.DomainModelUtils;
import org.janelia.model.jacs2.dao.DomainObjectDao;

public class DaoFactory {

    private final Instance<Dao<?, Number>> daosSource;

    @Inject
    public DaoFactory(@Any Instance<Dao<?, Number>> daosSource) {
        this.daosSource = daosSource;
    }

    public Dao<?, Number> createDao(String entityName) {
        Class<?> entityClass = DomainModelUtils.getBasePersistedEntityClass(entityName);
        for (Dao<?, Number> dao : daosSource) {
            Class<?> daoParameter = DomainModelUtils.getGenericParameterType(dao.getClass(), 0);
            if (entityClass.equals(daoParameter)) {
                return dao;
            }
        }
        throw new IllegalArgumentException("Unknown entity name: " + entityName);
    }

    public DomainObjectDao<?> createDomainObjectDao(String entityName) {
        Class<?> entityClass = DomainModelUtils.getBasePersistedEntityClass(entityName);
        List<Dao<?, Number>> assignableDaos = new LinkedList<>();
        for (Dao<?, Number> dao : daosSource) {
            Class<?> daoParameter = DomainModelUtils.getGenericParameterType(dao.getClass(), 0);
            if (entityClass.equals(daoParameter) && dao instanceof DomainObjectDao) {
                return (DomainObjectDao) dao;
            } else if (entityClass.isAssignableFrom(daoParameter) && dao instanceof DomainObjectDao) {
                assignableDaos.add(dao);
            }
        }
        if (assignableDaos.isEmpty()) {
            throw new IllegalArgumentException("Unknown or not a domain entity: " + entityName);
        } else if (assignableDaos.size() == 1) {
            return (DomainObjectDao) assignableDaos.get(0);
        } else {
            throw new IllegalArgumentException("Too many assignable DAOs found for: " + entityName);
        }
    }

}
