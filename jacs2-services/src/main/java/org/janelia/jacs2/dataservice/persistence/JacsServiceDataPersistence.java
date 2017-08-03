package org.janelia.jacs2.dataservice.persistence;

import com.google.common.collect.ImmutableMap;
import org.janelia.jacs2.dao.JacsServiceDataDao;
import org.janelia.jacs2.model.DataInterval;
import org.janelia.jacs2.model.EntityFieldValueHandler;
import org.janelia.jacs2.model.SetFieldValueHandler;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceEvent;
import org.janelia.jacs2.model.jacsservice.JacsServiceEventTypes;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.janelia.jacs2.model.page.PageRequest;
import org.janelia.jacs2.model.page.PageResult;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JacsServiceDataPersistence extends AbstractDataPersistence<JacsServiceDataDao, JacsServiceData, Number> {

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
                    jacsServiceDataDao.addServiceEvent(
                            jacsServiceData,
                            JacsServiceData.createServiceEvent(JacsServiceEventTypes.CREATE_CHILD_SERVICE, String.format("Created child service %s", jacsServiceData)));
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
            List<JacsServiceData> childServices = jacsServiceDataDao.findChildServices(jacsServiceData.getId());
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

    public void archiveHierarchy(JacsServiceData jacsServiceData) {
        JacsServiceDataDao jacsServiceDataDao = daoSource.get();
        try {
            jacsServiceDataDao.archiveServiceHierarchy(jacsServiceData);
        } finally {
            daoSource.destroy(jacsServiceDataDao);
        }
    }

    public void update(JacsServiceData jacsServiceData, Map<String, EntityFieldValueHandler<?>> fieldsToUpdate) {
        if (jacsServiceData.hasId()) {
            super.update(jacsServiceData, fieldsToUpdate);
        }
    }

    public void updateServiceState(JacsServiceData jacsServiceData, JacsServiceState newServiceState, Optional<JacsServiceEvent> serviceEvent) {
        JacsServiceState oldServiceState = jacsServiceData.getState();
        jacsServiceData.setState(newServiceState);
        if (newServiceState != oldServiceState) {
            addServiceEvent(
                    jacsServiceData,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.UPDATE_STATE, "Update state from " + oldServiceState + " -> " + newServiceState));
            if (jacsServiceData.hasId()) update(jacsServiceData, ImmutableMap.of("state", new SetFieldValueHandler<>(newServiceState)));
        }
        if (serviceEvent.isPresent()) addServiceEvent(jacsServiceData, serviceEvent.get());
    }

    public void addServiceEvent(JacsServiceData jacsServiceData, JacsServiceEvent serviceEvent) {
        jacsServiceData.addNewEvent(serviceEvent);
        if (jacsServiceData.hasId()) {
            JacsServiceDataDao jacsServiceDataDao = daoSource.get();
            try {
                jacsServiceDataDao.addServiceEvent(jacsServiceData, serviceEvent);
            } finally {
                daoSource.destroy(jacsServiceDataDao);
            }
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
