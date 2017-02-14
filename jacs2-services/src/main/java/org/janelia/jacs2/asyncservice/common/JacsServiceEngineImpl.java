package org.janelia.jacs2.asyncservice.common;

import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacs2.asyncservice.JacsServiceEngine;
import org.janelia.jacs2.asyncservice.ServerStats;
import org.janelia.jacs2.asyncservice.ServiceRegistry;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.slf4j.Logger;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.Semaphore;

public class JacsServiceEngineImpl implements JacsServiceEngine {
    private static final int MAX_RUNNING_SLOTS = 1000;

    private final JacsServiceQueue jacsServiceQueue;
    private final Instance<ServiceRegistry> serviceRegistrarSource;
    private final Logger logger;
    private int nAvailableSlots;
    private final Semaphore availableSlots;

    @Inject
    JacsServiceEngineImpl(JacsServiceQueue jacsServiceQueue,
                          Instance<ServiceRegistry> serviceRegistrarSource,
                          Logger logger) {
        this.jacsServiceQueue = jacsServiceQueue;
        this.serviceRegistrarSource = serviceRegistrarSource;
        this.logger = logger;
        nAvailableSlots = MAX_RUNNING_SLOTS;
        availableSlots = new Semaphore(nAvailableSlots, true);
    }

    @Override
    public ServerStats getServerStats() {
        ServerStats stats = new ServerStats();
        stats.setWaitingServices(jacsServiceQueue.getReadyServicesSize());
        stats.setAvailableSlots(getAvailableSlots());
        stats.setRunningServicesCount(jacsServiceQueue.getPendingServicesSize());
        stats.setRunningServices(jacsServiceQueue.getPendingServices());
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
    public ServiceProcessor<?> getServiceProcessor(JacsServiceData jacsServiceData) {
        ServiceDescriptor serviceDescriptor = getServiceDescriptor(jacsServiceData.getName());
        return serviceDescriptor.createServiceProcessor();
    }

    @Override
    public boolean acquireSlot() {
        return availableSlots.tryAcquire();
    }

    @Override
    public void releaseSlot() {

    }

    private ServiceDescriptor getServiceDescriptor(String serviceName) {
        ServiceRegistry registrar = serviceRegistrarSource.get();
        ServiceDescriptor serviceDescriptor = registrar.lookupService(serviceName);
        if (serviceDescriptor == null) {
            logger.error("No service found for {}", serviceName);
            throw new IllegalArgumentException("Unknown service: " + serviceName);
        }
        return serviceDescriptor;
    }

    @Override
    public JacsServiceData submitSingleService(JacsServiceData serviceArgs) {
        return jacsServiceQueue.enqueueService(serviceArgs);
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
            int currentPriority = currentService.priority();
            if (prevPriority >= 0) {
                int newPriority = prevPriority + 1;
                if (currentPriority < newPriority) {
                    currentPriority = newPriority;
                    currentService.updateServiceHierarchyPriority(currentPriority);
                }
            }
            prevPriority = currentPriority;
        }
        // submit the services and update their dependencies
        for (JacsServiceData currentService : listOfServices) {
            if (prevService != null) {
                currentService.addServiceDependency(prevService);
            }
            JacsServiceData submitted = jacsServiceQueue.enqueueService(currentService);
            results.add(submitted);
            prevService = submitted;
        }
        return results;
    }

}
