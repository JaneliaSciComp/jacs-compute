package org.janelia.jacs2.dataservice.persistence;

import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import com.google.common.collect.ImmutableMap;

import org.janelia.model.access.dao.DaoUpdateResult;
import org.janelia.model.access.dao.JacsServiceDataDao;
import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.jacs2.SetFieldValueHandler;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEvent;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.JacsServiceState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Dependent
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

    public PageResult<JacsServiceData> claimServiceByQueueAndState(String queueId, boolean onlyPreAssignedWork, Set<JacsServiceState> requestStates, PageRequest pageRequest) {
        JacsServiceDataDao jacsServiceDataDao = daoSource.get();
        try {
            return jacsServiceDataDao.claimServiceByQueueAndState(queueId, onlyPreAssignedWork, requestStates, pageRequest);
        } finally {
            daoSource.destroy(jacsServiceDataDao);
        }
    }

    public long countMatchingServices(JacsServiceData pattern, DataInterval<Date> creationInterval) {
        JacsServiceDataDao jacsServiceDataDao = daoSource.get();
        try {
            return jacsServiceDataDao.countMatchingServices(pattern, creationInterval);
        } finally {
            daoSource.destroy(jacsServiceDataDao);
        }
    }

    public PageResult<JacsServiceData> findMatchingServices(JacsServiceData pattern, DataInterval<Date> creationInterval, PageRequest pageRequest) {
        JacsServiceDataDao jacsServiceDataDao = daoSource.get();
        try {
            PageResult<JacsServiceData> results = jacsServiceDataDao.findMatchingServices(pattern, creationInterval, pageRequest);
            results.setTotalCount(jacsServiceDataDao.countMatchingServices(pattern, creationInterval));
            return results;
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

    public List<JacsServiceData> findDirectServiceDependencies(JacsServiceData jacsServiceData) {
        JacsServiceDataDao jacsServiceDataDao = daoSource.get();
        try {
            List<JacsServiceData> dependencies = jacsServiceDataDao.findByIds(jacsServiceData.getDependenciesIds());
            List<JacsServiceData> childServices = jacsServiceData.hasId() ? jacsServiceDataDao.findChildServices(jacsServiceData.getId()) : Collections.emptyList();
            return Stream.concat(
                    dependencies.stream(),
                    childServices.stream()
                    .filter(sd -> !jacsServiceData.getDependenciesIds().contains(sd.getId())))
                    .collect(Collectors.toList());
        } finally {
            daoSource.destroy(jacsServiceDataDao);
        }
    }

    public JacsServiceData findServiceHierarchy(JacsServiceData jacsServiceData) {
        if (jacsServiceData.hasId()) {
            return findServiceHierarchy(jacsServiceData.getId());
        } else {
            return jacsServiceData;
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

    public Optional<Boolean> update(JacsServiceData jacsServiceData, Map<String, EntityFieldValueHandler<?>> fieldsToUpdate) {
        if (jacsServiceData.hasId()) {
            return super.update(jacsServiceData, fieldsToUpdate);
        } else {
            return Optional.empty();
        }
    }

    public Optional<Boolean> updateServiceState(JacsServiceData jacsServiceData, JacsServiceState newServiceState, JacsServiceEvent serviceEvent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Update service state for {} to {} with {}", jacsServiceData, newServiceState,
                    (serviceEvent != JacsServiceEvent.NO_EVENT ? "event " + serviceEvent : "no event"));
        } else {
            LOG.info("Update service state for {} to {} with {}", jacsServiceData.getShortName(), newServiceState,
                    (serviceEvent != JacsServiceEvent.NO_EVENT ? "event " + serviceEvent : "no event"));
        }
        Map<String, EntityFieldValueHandler<?>> serviceUpdates = new LinkedHashMap<>(jacsServiceData.updateState(newServiceState));
        if (serviceEvent != JacsServiceEvent.NO_EVENT) {
            serviceUpdates.putAll(jacsServiceData.addNewEvent(serviceEvent));
        }
        if (jacsServiceData.hasId() && !serviceUpdates.isEmpty()) {
            JacsServiceDataDao jacsServiceDataDao = daoSource.get();
            try {
                DaoUpdateResult updateResult = jacsServiceDataDao.update(jacsServiceData, serviceUpdates);
                return Optional.of(updateResult.getEntitiesFound() > 0 && updateResult.getEntitiesAffected() > 0);
            } finally {
                daoSource.destroy(jacsServiceDataDao);
            }
        } else {
            return Optional.empty();
        }
    }

    public Optional<Boolean> addServiceEvent(JacsServiceData jacsServiceData, JacsServiceEvent serviceEvent) {
        JacsServiceDataDao jacsServiceDataDao = daoSource.get();
        try {
            return addServiceEvent(jacsServiceDataDao, jacsServiceData, serviceEvent);
        } finally {
            daoSource.destroy(jacsServiceDataDao);
        }
    }

    private Optional<Boolean> addServiceEvent(JacsServiceDataDao jacsServiceDataDao, JacsServiceData jacsServiceData, JacsServiceEvent serviceEvent) {
        Map<String, EntityFieldValueHandler<?>> jacsServiceDataUpdates = jacsServiceData.addNewEvent(serviceEvent);
        if (jacsServiceData.hasId()) {
            DaoUpdateResult updateResult = jacsServiceDataDao.update(jacsServiceData, jacsServiceDataUpdates);
            return Optional.of(updateResult.getEntitiesFound() > 0 && updateResult.getEntitiesAffected() > 0);
        } else {
            return Optional.empty();
        }
    }

    public Optional<Boolean> updateServiceResult(JacsServiceData jacsServiceData) {
        if (jacsServiceData.hasId()) {
            JacsServiceDataDao jacsServiceDataDao = daoSource.get();
            try {
                DaoUpdateResult updateResult = jacsServiceDataDao.update(jacsServiceData,
                        ImmutableMap.of(
                                "serializableResult",
                                new SetFieldValueHandler<>(jacsServiceData.getSerializableResult())));
                return Optional.of(updateResult.getEntitiesFound() > 0 && updateResult.getEntitiesAffected() > 0);
            } finally {
                daoSource.destroy(jacsServiceDataDao);
            }
        } else {
            return Optional.empty();
        }
    }

}
