package org.janelia.jacs2.asyncservice.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.janelia.jacs2.asyncservice.ServiceRegistry;
import org.janelia.jacs2.asyncservice.common.ServiceProcessor;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

@Dependent
public class JacsServiceRegistry implements ServiceRegistry {

    private final Instance<ServiceProcessor<?>> anyServiceSource;
    private final Logger logger;

    @Inject
    public JacsServiceRegistry(@Any Instance<ServiceProcessor<?>> anyServiceSource, Logger logger) {
        this.anyServiceSource = anyServiceSource;
        this.logger = logger;
    }

    @Override
    public ServiceMetaData getServiceMetadata(String serviceName) {
        ServiceProcessor service = lookupService(serviceName);
        return service != null ? service.getMetadata() : null;
    }

    @Override
    public List<ServiceMetaData> getAllServicesMetadata() {
        return getAllServices(anyServiceSource).stream().map(ServiceProcessor::getMetadata).collect(Collectors.toList());
    }

    @Override
    public ServiceProcessor<?> lookupService(String serviceName) {
        try {
            for (ServiceProcessor<?> service : getAllServices(anyServiceSource)) {
                if (service.getMetadata().getServiceName().equals(serviceName.trim())) {
                    logger.trace("Service found for {}", serviceName);
                    return service;
                }
            }
            logger.error("NO Service found for {}", serviceName);
        } catch (Throwable e) {
            logger.error("Error while looking up {}", serviceName, e);
        }
        return null;
    }

    private List<ServiceProcessor<?>> getAllServices(@Any Instance<ServiceProcessor<?>> services) {
        List<ServiceProcessor<?>> allServices = new ArrayList<>();
        for (ServiceProcessor<?> service : services) {
            allServices.add(service);
        }
        return allServices;
    }

}
