package org.janelia.jacs2.dataservice.persistence;

import org.janelia.model.access.dao.JacsServiceDataDao;
import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEvent;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.JacsServiceState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JacsServiceDataPersistence extends AbstractDataPersistence<JacsServiceDataDao, JacsServiceData, Number> {

    private static final Logger LOG = LoggerFactory.getLogger(JacsServiceDataPersistence.class);

    @Inject
    public JacsServiceDataPersistence(Instance<JacsServiceDataDao> serviceDataDaoSource) {
        super(serviceDataDaoSource);
    }

    public JacsServiceData createServiceIfNotFound(JacsServiceData jacsServiceData) {
        JacsServiceDataDao jacsServiceDataDao = daoSource.get();
        try {
            JacsServiceData parentServiceData = jacsServiceDataDao.findServiceHierarchy(jacsServiceData.getParentServiceId());
            if (parentServiceData == null) {
                jacsServiceDataDao.saveServiceHierarchy(jacsServiceData);
                return jacsServiceData;
            } else {
                Optional<JacsServiceData> existingInstance = parentServiceData.findSimilarDependency(jacsServiceData);
                if (existingInstance.isPresent()) {
                    return existingInstance.get();
                } else {
                    jacsServiceDataDao.saveServiceHierarchy(jacsServiceData);
                    addServiceEvent(
                            jacsServiceDataDao,
                            jacsServiceData,
                            JacsServiceData.createServiceEvent(JacsServiceEventTypes.CREATE_CHILD_SERVICE, String.format("Created child service %s", jacsServiceData))
                    );
                    return jacsServiceData;
                }
            }
        } finally {
            daoSource.destroy(jacsServiceDataDao);
        }
    }

    public PageResult<JacsServiceData> claimServiceByQueueAndState(String queueId, Set<JacsServiceState> requestStates, PageRequest pageRequest) {
        JacsServiceDataDao jacsServiceDataDao = daoSource.get();
        try {
            return jacsServiceDataDao.claimServiceByQueueAndState(queueId, requestStates, pageRequest);
        } finally {
            daoSource.destroy(jacsServiceDataDao);
        }
    }

    public PageResult<JacsServiceData> findMatchingServices(JacsServiceData pattern, DataInterval<Date> creationInterval, PageRequest pageRequest) {
        JacsServiceDataDao jacsServiceDataDao = daoSource.get();
        try {
            return jacsServiceDataDao.findMatchingServices(pattern, creationInterval, pageRequest);
        } finally {
            daoSource.destroy(jacsServiceDataDao);
        }
    }

    public PageResult<JacsServiceData> findServicesByState(Set<JacsServiceState> requestStates, PageRequest pageRequest) {
        JacsServiceDataDao jacsServiceDataDao = daoSource.get();
        try {
            return jacsServiceDataDao.findServicesByState(requestStates, pageRequest);
        } finally {
            daoSource.destroy(jacsServiceDataDao);
        }
    }

    public List<JacsServiceData> findChildServices(Number serviceId) {
        JacsServiceDataDao jacsServiceDataDao = daoSource.get();
        try {
            return jacsServiceDataDao.findChildServices(serviceId);
        } finally {
            daoSource.destroy(jacsServiceDataDao);
        }
    }

    public List<JacsServiceData> findServiceDependencies(JacsServiceData jacsServiceData) {
        JacsServiceDataDao jacsServiceDataDao = daoSource.get();
        try {
            List<JacsServiceData> dependencies = jacsServiceDataDao.findByIds(jacsServiceData.getDependenciesIds());
            List<JacsServiceData> childServices = jacsServiceData.hasId() ? jacsServiceDataDao.findChildServices(jacsServiceData.getId()) : Collections.emptyList();
            return Stream.concat(dependencies.stream(), childServices.stream()).collect(Collectors.toList());
        } finally {
            daoSource.destroy(jacsServiceDataDao);
        }
    }

    public JacsServiceData findServiceHierarchy(Number serviceId) {
        JacsServiceDataDao jacsServiceDataDao = daoSource.get();
        try {
            return jacsServiceDataDao.findServiceHierarchy(serviceId);
        } finally {
            daoSource.destroy(jacsServiceDataDao);
        }
    }

    public void saveHierarchy(JacsServiceData jacsServiceData) {
        JacsServiceDataDao jacsServiceDataDao = daoSource.get();
        try {
            jacsServiceDataDao.saveServiceHierarchy(jacsServiceData);
        } finally {
            daoSource.destroy(jacsServiceDataDao);
        }
    }

    public void update(JacsServiceData jacsServiceData, Map<String, EntityFieldValueHandler<?>> fieldsToUpdate) {
        if (jacsServiceData.hasId()) {
            super.update(jacsServiceData, fieldsToUpdate);
        }
    }

    public void updateServiceState(JacsServiceData jacsServiceData, JacsServiceState newServiceState, JacsServiceEvent serviceEvent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Update service state for {} to {} with event {}", jacsServiceData, newServiceState, serviceEvent);
        } else {
            LOG.info("Update service state for {} to {} with event {}", jacsServiceData.getId(), newServiceState, serviceEvent);
        }
        List<Consumer<JacsServiceDataDao>> actions = new ArrayList<>();
        actions.add(dao -> dao.update(jacsServiceData, jacsServiceData.updateState(newServiceState)));
        if (serviceEvent != JacsServiceEvent.NO_EVENT) {
            actions.add(dao -> dao.update(jacsServiceData, jacsServiceData.addNewEvent(serviceEvent)));
        }
        if (jacsServiceData.hasId() && !actions.isEmpty()) {
            JacsServiceDataDao jacsServiceDataDao = daoSource.get();
            try {
                actions.forEach(action -> action.accept(jacsServiceDataDao));
            } finally {
                daoSource.destroy(jacsServiceDataDao);
            }
        }
    }

    public void addServiceEvent(JacsServiceData jacsServiceData, JacsServiceEvent serviceEvent) {
        JacsServiceDataDao jacsServiceDataDao = daoSource.get();
        try {
            addServiceEvent(jacsServiceDataDao, jacsServiceData, serviceEvent);
        } finally {
            daoSource.destroy(jacsServiceDataDao);
        }
    }

    private void addServiceEvent(JacsServiceDataDao jacsServiceDataDao, JacsServiceData jacsServiceData, JacsServiceEvent serviceEvent) {
        Map<String, EntityFieldValueHandler<?>> jacsServiceDataUpdates = jacsServiceData.addNewEvent(serviceEvent);
        if (jacsServiceData.hasId()) {
            jacsServiceDataDao.update(jacsServiceData, jacsServiceDataUpdates);
        }
    }

    public void updateServiceResult(JacsServiceData jacsServiceData) {
        if (jacsServiceData.hasId()) {
            JacsServiceDataDao jacsServiceDataDao = daoSource.get();
            try {
                jacsServiceDataDao.updateServiceResult(jacsServiceData);
            } finally {
                daoSource.destroy(jacsServiceDataDao);
            }
        }
    }

}
