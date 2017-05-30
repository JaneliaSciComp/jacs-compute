package org.janelia.jacs2.dataservice.persistence;

import com.google.common.collect.ImmutableMap;
import org.janelia.jacs2.dao.JacsServiceDataDao;
import org.janelia.jacs2.model.DataInterval;
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

public class JacsServiceDataPersistence extends AbstractDataPersistence<JacsServiceDataDao, JacsServiceData, Number> {

    @Inject
    public JacsServiceDataPersistence(Instance<JacsServiceDataDao> serviceDataDaoSource) {
        super(serviceDataDaoSource);
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

    public void update(JacsServiceData jacsServiceData, Map<String, Object> fieldsToUpdate) {
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
            if (jacsServiceData.hasId()) update(jacsServiceData, ImmutableMap.of("state", newServiceState));
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
