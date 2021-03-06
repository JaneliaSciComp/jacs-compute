package org.janelia.jacs2.asyncservice.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacs2.cdi.qualifier.BoolPropertyValue;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.jacs2.page.SortCriteria;
import org.janelia.model.jacs2.page.SortDirection;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.JacsServiceState;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;

@ApplicationScoped
public class InMemoryJacsServiceQueue implements JacsServiceQueue {
    private static final Object ACCESS_LOCK = new Object();

    private static final int DEFAULT_MAX_READY_CAPACITY = 20;
    private static final long MAX_WAIT_IN_SUBMIT_STATE_MILLIS = 300000; // 5min

    private JacsServiceDataPersistence jacsServiceDataPersistence;
    private Queue<JacsServiceData> waitingServices;
    private final Set<Number> waitingServicesSet = new LinkedHashSet<>();
    private final Set<Number> submittedServicesSet = new LinkedHashSet<>();
    private Logger logger;
    private String queueId;
    private boolean onlyPreAssignedWork;
    private int maxReadyCapacity;
    private boolean noWaitingSpaceAvailable;

    InMemoryJacsServiceQueue() {
        // CDI required ctor
    }

    @Inject
    public InMemoryJacsServiceQueue(JacsServiceDataPersistence jacsServiceDataPersistence,
                                    @PropertyValue(name = "service.queue.id") String queueId,
                                    @BoolPropertyValue(name = "service.queue.getOnlyPreAssignedWork") boolean onlyPreAssignedWork,
                                    @PropertyValue(name = "service.queue.MaxCapacity") int maxReadyCapacity,
                                    Logger logger) {
        this.queueId = queueId;
        this.onlyPreAssignedWork = onlyPreAssignedWork;
        this.maxReadyCapacity = maxReadyCapacity < 0 ? 0 : maxReadyCapacity;
        this.jacsServiceDataPersistence = jacsServiceDataPersistence;
        this.waitingServices = new PriorityBlockingQueue<>(
                this.maxReadyCapacity == 0 ? DEFAULT_MAX_READY_CAPACITY : this.maxReadyCapacity,
                new DefaultServiceInfoComparator()
        );
        this.logger = logger;
        noWaitingSpaceAvailable = false;
    }

    @Override
    public JacsServiceData enqueueService(JacsServiceData jacsServiceData) {
        logger.debug("Enqueued service {}", jacsServiceData);
        if (noWaitingSpaceAvailable) {
            // don't even check if anything has become available since last time
            // just drop it for now - the queue will be refilled after it drains.
            logger.info("In memory queue reached the capacity so service {} will not be put in memory", jacsServiceData);
            return jacsServiceData;
        }
        boolean added = addWaitingService(jacsServiceData);
        noWaitingSpaceAvailable  = !added || (waitingCapacity() <= 0);
        if (noWaitingSpaceAvailable) {
            logger.info("Not enough space in memory queue for {}", jacsServiceData);
        }
        return jacsServiceData;
    }

    @Override
    public JacsServiceData dequeService() {
        synchronized (ACCESS_LOCK) {
            JacsServiceData queuedService = getWaitingService();
            if (queuedService == null && enqueueAvailableServices(EnumSet.of(
                    JacsServiceState.CREATED, JacsServiceState.QUEUED, JacsServiceState.RESUMED, JacsServiceState.RETRY))) {
                queuedService = getWaitingService();
            }
            return queuedService;
        }
    }

    @Override
    public void refreshServiceQueue() {
        logger.trace("Sync the waiting queue");
        // check for newly created services and queue them based on their priorities
        enqueueAvailableServices(EnumSet.of(JacsServiceState.CREATED));
    }

    @Override
    public void abortService(JacsServiceData jacsServiceData) {
        synchronized (ACCESS_LOCK) {
            submittedServicesSet.remove(jacsServiceData.getId());
        }
    }

    @Override
    public void completeService(JacsServiceData jacsServiceData) {
        synchronized (ACCESS_LOCK) {
            submittedServicesSet.remove(jacsServiceData.getId());
        }
    }

    @Override
    public int getReadyServicesSize() {
        synchronized (ACCESS_LOCK) {
            return waitingServices.size();
        }
    }

    @Override
    public int getPendingServicesSize() {
        synchronized (ACCESS_LOCK) {
            return submittedServicesSet.size();
        }
    }

    @Override
    public List<Number> getPendingServices() {
        synchronized (ACCESS_LOCK) {
            return ImmutableList.copyOf(submittedServicesSet);
        }
    }

    @Override
    public int getMaxReadyCapacity() {
        return maxReadyCapacity;
    }

    @Override
    public void setMaxReadyCapacity(int maxReadyCapacity) {
        this.maxReadyCapacity = maxReadyCapacity <=0 ? 0 : maxReadyCapacity;
    }

    private boolean addWaitingService(JacsServiceData jacsServiceData) {
        synchronized (ACCESS_LOCK) {
            if (waitingCapacity() == 0 && !jacsServiceData.hasParentServiceId()) {
                // don't enqueue root services if no spaces are available
                // and if they already are in memory abort them if no slots are available
                abortService(jacsServiceData);
                return false;
            }
            if (isInMemory(jacsServiceData)) {
                return true;
            }
            boolean added = waitingServices.offer(jacsServiceData);
            if (added) {
                logger.debug("Enqueued service {} into {}", jacsServiceData, this);
                waitingServicesSet.add(jacsServiceData.getId());
                if (jacsServiceData.getState() == JacsServiceState.CREATED) {
                    jacsServiceDataPersistence.updateServiceState(
                            jacsServiceData,
                            JacsServiceState.QUEUED,
                            JacsServiceData.createServiceEvent(JacsServiceEventTypes.QUEUED, String.format("Waiting to be processed on '%s'", queueId)));
                }
            }
            return added;
        }
    }

    private boolean enqueueAvailableServices(Set<JacsServiceState> jacsServiceStates) {
        synchronized (ACCESS_LOCK) {
            int availableSpaces = maxReadyCapacity;
            PageRequest servicePageRequest = new PageRequest();
            servicePageRequest.setPageSize(availableSpaces);
            servicePageRequest.setSortCriteria(new ArrayList<>(ImmutableList.of(
                    new SortCriteria("priority", SortDirection.DESC),
                    new SortCriteria("creationDate"))));
            PageResult<JacsServiceData> services = jacsServiceDataPersistence.claimServiceByQueueAndState(queueId, onlyPreAssignedWork, jacsServiceStates, servicePageRequest);
            if (CollectionUtils.isNotEmpty(services.getResultList())) {
                services.getResultList().stream().forEach(serviceData -> {
                    try {
                        Preconditions.checkArgument(serviceData.getId() != null, "Invalid service ID");
                        addWaitingService(serviceData);
                    } catch (Exception e) {
                        logger.error("Internal error - no computation can be created for {}", serviceData);
                    }
                });
                noWaitingSpaceAvailable = waitingCapacity() <= 0;
                return true;
            }
            return false;
        }
    }

    private JacsServiceData getWaitingService() {
        synchronized (ACCESS_LOCK) {
            JacsServiceData jacsServiceData = waitingServices.poll();
            if (jacsServiceData != null) {
                logger.debug("Retrieved waiting service {}", jacsServiceData);
                Number serviceId = jacsServiceData.getId();
                submittedServicesSet.add(serviceId);
                waitingServicesSet.remove(serviceId);
            }
            return jacsServiceData;
        }
    }

    private int waitingCapacity() {
        int remainingCapacity = maxReadyCapacity - waitingServices.size();
        return remainingCapacity < 0 ? 0 : remainingCapacity;
    }

    private boolean isInMemory(JacsServiceData jacsServiceData) {
        Number jacsServiceId = jacsServiceData.getId();
        if (waitingServicesSet.contains(jacsServiceId)) {
            logger.debug("Service {} already waiting in the queue {}", jacsServiceData, this);
            return true;
        } else if (submittedServicesSet.contains(jacsServiceId)) {
            if (EnumSet.of(JacsServiceState.CREATED, JacsServiceState.QUEUED, JacsServiceState.RESUMED, JacsServiceState.RETRY).contains(jacsServiceData.getState())) {
                if (jacsServiceData.getModificationDate() != null && System.currentTimeMillis() - jacsServiceData.getModificationDate().getTime() > MAX_WAIT_IN_SUBMIT_STATE_MILLIS) {
                    // something is not quite right since the service hasn't been picked up yet
                    // so abandon the service
                    logger.info("Abort service {} from {} because it's been waiting for too long", jacsServiceData, this);
                    abortService(jacsServiceData);
                    return false;
                } else {
                    return true;
                }
            } else {
                logger.debug("Service {} already found in the queue {} as submitted", jacsServiceData, this);
                return true;
            }
        } else {
            return false;
        }
    }
}
