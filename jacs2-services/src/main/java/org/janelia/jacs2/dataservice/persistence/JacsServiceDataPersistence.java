package org.janelia.jacs2.dataservice.persistence;

import com.google.common.collect.ImmutableMap;
import org.janelia.model.access.dao.JacsServiceDataDao;
import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.jacs2.SetFieldValueHandler;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEvent;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.JacsServiceState;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;

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
        List<Consumer<JacsServiceDataDao>> actions = new ArrayList<>();
        if (newServiceState != oldServiceState) {
            jacsServiceData.setState(newServiceState);
            JacsServiceEvent updateStateEvent = JacsServiceData.createServiceEvent(JacsServiceEventTypes.UPDATE_STATE, "Update state from " + oldServiceState + " -> " + newServiceState);
            jacsServiceData.addNewEvent(updateStateEvent);

            actions.add(dao -> dao.addServiceEvent(jacsServiceData, updateStateEvent));
            actions.add(dao -> dao.update(jacsServiceData, ImmutableMap.of("state", new SetFieldValueHandler<>(newServiceState))));
        }
        serviceEvent.ifPresent(anotherEvent -> {
            jacsServiceData.addNewEvent(anotherEvent);
            actions.add(dao -> dao.addServiceEvent(jacsServiceData, anotherEvent));
        });
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
