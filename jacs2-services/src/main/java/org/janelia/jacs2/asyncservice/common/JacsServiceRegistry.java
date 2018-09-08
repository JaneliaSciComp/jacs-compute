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
        return getBeanByName(ServiceProcessor.class, serviceName);
    }

    private List<ServiceProcessor<?>> getAllServices(@Any Instance<ServiceProcessor<?>> services) {
        List<ServiceProcessor<?>> allServices = new ArrayList<>();
        for (ServiceProcessor<?> service : services) {
            allServices.add(service);
        }
        return allServices;
    }

    public ServiceInterceptor lookupInterceptor(String interceptorName) {
        return getBeanByName(ServiceInterceptor.class, interceptorName);
    }

    @SuppressWarnings("unchecked")
    public <T> T getBeanByName(Class<T> beanType, String beanName) {

        // Find beans with the given type and name
        BeanManager bm = CDI.current().getBeanManager();
        Set<Bean<?>> beans = bm.getBeans(beanName).stream()
                .filter(b -> beanType.isAssignableFrom(b.getBeanClass()))
                .collect(Collectors.toSet());

        if (beans==null || beans.isEmpty()) {
            logger.error("No service found with name '{}'", beanName);
            return null;
        }

        if (beans.size()>1) {
            logger.warn("More than one service found with name '{}'. Choosing one at random!", beanName);
        }

        // Get an instance
        Bean<T> bean = (Bean<T>) beans.iterator().next();
        CreationalContext<T> ctx = bm.createCreationalContext(bean);
        return (T) bm.getReference(bean, bean.getBeanClass(), ctx);
    }
}
