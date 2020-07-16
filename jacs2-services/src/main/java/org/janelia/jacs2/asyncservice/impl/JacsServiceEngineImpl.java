package org.janelia.jacs2.asyncservice.impl;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.JacsServiceEngine;
import org.janelia.jacs2.asyncservice.ServerStats;
import org.janelia.jacs2.asyncservice.ServiceRegistry;
import org.janelia.jacs2.asyncservice.common.JacsServiceQueue;
import org.janelia.jacs2.asyncservice.common.ServiceProcessor;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEvent;
import org.janelia.model.service.JacsServiceState;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

@ApplicationScoped
public class JacsServiceEngineImpl implements JacsServiceEngine {
    private static final int DEFAULT_MAX_RUNNING_SLOTS = 1000;

    private static final Map<JacsServiceState, Set<JacsServiceState>> VALID_TRANSITIONS =
            ImmutableMap.<JacsServiceState, Set<JacsServiceState>>builder()
                    .put(JacsServiceState.CREATED, EnumSet.of(
                            JacsServiceState.QUEUED,
                            JacsServiceState.SUSPENDED,
                            JacsServiceState.CANCELED, JacsServiceState.TIMEOUT
                    ))
                    .put(JacsServiceState.QUEUED, EnumSet.of(
                            JacsServiceState.DISPATCHED,
                            JacsServiceState.SUSPENDED,
                            JacsServiceState.CANCELED, JacsServiceState.TIMEOUT
                    ))
                    .put(JacsServiceState.DISPATCHED, EnumSet.of(
                            JacsServiceState.RUNNING,
                            JacsServiceState.WAITING_FOR_DEPENDENCIES,
                            JacsServiceState.SUSPENDED,
                            JacsServiceState.CANCELED, JacsServiceState.TIMEOUT
                    ))
                    .put(JacsServiceState.RUNNING,  EnumSet.of(
                            JacsServiceState.WAITING_FOR_DEPENDENCIES,
                            JacsServiceState.SUSPENDED,
                            JacsServiceState.CANCELED, JacsServiceState.TIMEOUT
                    ))
                    .put(JacsServiceState.WAITING_FOR_DEPENDENCIES, EnumSet.of(
                            JacsServiceState.RUNNING,
                            JacsServiceState.WAITING_FOR_DEPENDENCIES,
                            JacsServiceState.SUSPENDED,
                            JacsServiceState.CANCELED, JacsServiceState.TIMEOUT
                    ))
                    .put(JacsServiceState.CANCELED, EnumSet.of(JacsServiceState.RETRY)) // for retry
                    .put(JacsServiceState.TIMEOUT, EnumSet.of(JacsServiceState.RETRY)) // for retry
                    .put(JacsServiceState.ERROR, EnumSet.of(JacsServiceState.RETRY)) // for retry
                    .put(JacsServiceState.SUCCESSFUL, Collections.emptySet())
                    .put(JacsServiceState.SUSPENDED, EnumSet.of(JacsServiceState.RESUMED))
                    .put(JacsServiceState.RESUMED, EnumSet.of(
                            JacsServiceState.DISPATCHED,
                            JacsServiceState.SUSPENDED,
                            JacsServiceState.CANCELED, JacsServiceState.TIMEOUT))
                    .build();

    private JacsServiceDataPersistence jacsServiceDataPersistence;
    private JacsServiceQueue jacsServiceQueue;
    private Instance<ServiceRegistry> serviceRegistrarSource;
    private Logger logger;
    private int nAvailableSlots;
    private Semaphore availableSlots;
    private String defaultSubmissionQueueId;

    JacsServiceEngineImpl() {
        // CDI required ctor
    }

    @Inject
    public JacsServiceEngineImpl(JacsServiceDataPersistence jacsServiceDataPersistence,
                                 JacsServiceQueue jacsServiceQueue,
                                 Instance<ServiceRegistry> serviceRegistrarSource,
                                 @PropertyValue(name = "service.engine.ProcessingSlots") int nAvailableSlots,
                                 @PropertyValue(name = "service.defaultSubmission.queue.id") String defaultSubmissionQueueId,
                                 Logger logger) {
        this.jacsServiceDataPersistence = jacsServiceDataPersistence;
        this.jacsServiceQueue = jacsServiceQueue;
        this.serviceRegistrarSource = serviceRegistrarSource;
        this.logger = logger;
        this.nAvailableSlots = nAvailableSlots <= 0 ? DEFAULT_MAX_RUNNING_SLOTS : nAvailableSlots;
        this.availableSlots = new Semaphore(this.nAvailableSlots, true);
        this.defaultSubmissionQueueId = StringUtils.defaultIfBlank(defaultSubmissionQueueId, null);
        logger.info("Default queue id: {}", defaultSubmissionQueueId);
    }

    @Override
    public ServerStats getServerStats() {
        ServerStats stats = new ServerStats();

        stats.setAvailableSlots(getAvailableSlots());
        stats.setWaitingCapacity(jacsServiceQueue.getMaxReadyCapacity());
        stats.setWaitingServicesCount(jacsServiceQueue.getReadyServicesSize());
        stats.setRunningServicesCount(jacsServiceQueue.getPendingServicesSize());

        return stats;
    }

    private int getAvailableSlots() {
        return availableSlots.availablePermits();
    }

    @Override
    public void setProcessingSlotsCount(int nProcessingSlots) {
        int nDiff = nProcessingSlots - nAvailableSlots;
        if (nDiff > 0) {
            availableSlots.release(nDiff);
            nAvailableSlots = nProcessingSlots;
        } else if (nDiff < 0) {
            if (availableSlots.tryAcquire(-nDiff)) {
                nAvailableSlots = nProcessingSlots;
            }
        }
    }

    @Override
    public void setMaxWaitingSlots(int maxWaitingSlots) {
        jacsServiceQueue.setMaxReadyCapacity(maxWaitingSlots);
    }

    @Override
    public ServiceProcessor<?> getServiceProcessor(JacsServiceData jacsServiceData) {
        return getServiceProcessor(jacsServiceData.getName());
    }

    @Override
    public boolean acquireSlot() {
        return availableSlots.tryAcquire();
    }

    @Override
    public void releaseSlot() {
        availableSlots.release();
    }

    private ServiceProcessor<?> getServiceProcessor(String serviceName) {
        ServiceRegistry registrar = serviceRegistrarSource.get();
        ServiceProcessor<?> serviceProcessor = registrar.lookupService(serviceName);
        if (serviceProcessor == null) {
            logger.error("No service found for {}", serviceName);
            throw new IllegalArgumentException("Unknown service: " + serviceName);
        }
        return serviceProcessor;
    }

    @Override
    public JacsServiceData submitSingleService(JacsServiceData serviceArgs) {
        if (serviceArgs.getState() == null) {
            serviceArgs.setState(JacsServiceState.CREATED);
        }
        if (StringUtils.isBlank(serviceArgs.getQueueId())) {
            // if the client hasn't specified where the service should process
            // use the defaultProcessorQueueId to specify where processing will take place
            serviceArgs.setQueueId(defaultSubmissionQueueId);
        }
        serviceArgs.initAccessId();
        jacsServiceDataPersistence.saveHierarchy(serviceArgs);
        return serviceArgs;
    }

    @Override
    public List<JacsServiceData> submitMultipleServices(List<JacsServiceData> listOfServices) {
        if (CollectionUtils.isEmpty(listOfServices)) {
            return listOfServices;
        }
        JacsServiceData prevService = null;
        List<JacsServiceData> results = new ArrayList<>();
        // update the service priorities so that the priorities descend for subsequent services
        int prevPriority = -1;
        for (ListIterator<JacsServiceData> servicesItr = listOfServices.listIterator(listOfServices.size()); servicesItr.hasPrevious();) {
            JacsServiceData currentService = servicesItr.previous();
            currentService.initAccessId();
            int currentPriority = currentService.priority();
            if (prevPriority >= 0) {
                int newPriority = prevPriority + 1;
                if (currentPriority < newPriority) {
                    currentPriority = newPriority;
                    currentService.updateServicePriority(currentPriority);
                }
            }
            prevPriority = currentPriority;
        }
        // submit the services and update their dependencies
        for (JacsServiceData currentService : listOfServices) {
            if (prevService != null) {
                currentService.addServiceDependency(prevService);
            }
            JacsServiceData submitted = submitSingleService(currentService);
            results.add(submitted);
            prevService = submitted;
        }
        return results;
    }

    @Override
    public JacsServiceData updateServiceState(JacsServiceData serviceData, JacsServiceState serviceState, boolean forceFlag) {
        return serviceHierarchyStateUpdate(serviceData, serviceState, forceFlag);
    }

    private JacsServiceData serviceHierarchyStateUpdate(JacsServiceData serviceData, JacsServiceState serviceState, boolean forceFlag) {
        serviceData.serviceHierarchyStream()
                .filter(sd -> isTransitionValid(sd.getState(), serviceState, forceFlag))
                .sorted((s1, s2) -> {
                    // order them so that the ones that don't have any dependencies are first
                    if (!s1.getDependenciesIds().contains(s2.getId())) {
                        return -1;
                    } else if (!s2.getDependenciesIds().contains(s1.getId())) {
                        return 1;
                    } else {
                        return 0;
                    }
                })
                .forEach(sd -> {
                    singleServiceStateUpdate(sd, serviceState, forceFlag);
                });
        return jacsServiceDataPersistence.findServiceHierarchy(serviceData.getId());
    }

    private JacsServiceData singleServiceStateUpdate(JacsServiceData serviceData, JacsServiceState serviceState, boolean forceFlag) {
        if (sameState(serviceData.getState(), serviceState) || isTransitionInvalid(serviceData.getState(), serviceState, forceFlag)) {
            logger.info("State unchanged or invalid transition from {} to {} for {} without forcing it", serviceData.getState(), serviceState, serviceData);
            return serviceData;
        }
        return jacsServiceDataPersistence.updateServiceState(serviceData, serviceState, JacsServiceEvent.NO_EVENT)
                .map(updateResult -> {
                    if (updateResult) {
                        return serviceData;
                    } else {
                        return jacsServiceDataPersistence.findById(serviceData.getId());
                    }
                })
                .orElseThrow(() -> new IllegalArgumentException("Invalid service data"));
    }

    private boolean sameState(JacsServiceState s1, JacsServiceState s2) {
        return s1 != null && s1 == s2;
    }

    private boolean isTransitionValid(JacsServiceState from, JacsServiceState to, boolean forceFlag) {
        return to != null &&
                (from == to ||
                        forceFlag ||
                        VALID_TRANSITIONS.get(from) != null && VALID_TRANSITIONS.get(from).contains(to));
    }

    private boolean isTransitionInvalid(JacsServiceState from, JacsServiceState to, boolean forceFlag) {
        return !isTransitionValid(from, to, forceFlag);
    }

}
