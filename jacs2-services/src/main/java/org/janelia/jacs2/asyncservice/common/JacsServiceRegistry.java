package org.janelia.jacs2.asyncservice.common;

import org.janelia.jacs2.asyncservice.ServiceRegistry;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.CDI;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class JacsServiceRegistry implements ServiceRegistry {

    private final Instance<ServiceProcessor<?>> anyServiceSource;
    private final Logger logger;

    @Inject
    public JacsServiceRegistry(@Any Instance<ServiceProcessor<?>> anyServiceSource,
                               Logger logger) {
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
                try {
                    if (serviceName.equals(service.getMetadata().getServiceName())) {
                        logger.trace("Service found: {}", serviceName);
                        return service;
                    }
                }
                catch (Exception e) {
                    logger.error("Error reading service metadata for: "+service.getClass().getName(), e);
                }
            }
            logger.error("No service found with name '{}'", serviceName);
        }
        catch (Throwable e) {
            logger.error("Error while looking up service '{}'", serviceName, e);
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

    public ServiceInterceptor lookupInterceptor(String interceptorName) {
        BeanManager bm = CDI.current().getBeanManager();
        Set<Bean<?>> beans = bm.getBeans(interceptorName);
        if (beans==null || beans.isEmpty()) {
            logger.error("No interceptor found with name '{}'", interceptorName);
            return null;
        }
        Bean<ServiceInterceptor> bean = (Bean<ServiceInterceptor>) bm.getBeans(ServiceInterceptor.class).iterator().next();
        CreationalContext<ServiceInterceptor> ctx = bm.createCreationalContext(bean);
        return (ServiceInterceptor) bm.getReference(bean, ServiceInterceptor.class, ctx);
    }
}
