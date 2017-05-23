package org.janelia.jacs2.cdi;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceComputationQueue;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.cdi.qualifier.TaskQueuePoll;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

@ApplicationScoped
public class ApplicationProducer {

    @Produces
    @ApplicationScoped
    public ObjectMapperFactory objectMapperFactory() {
        return ObjectMapperFactory.instance();
    }

    @Produces
    public ObjectMapper objectMapper() {
        return ObjectMapperFactory.instance().getDefaultObjectMapper();
    }

    @JacsDefault
    @Produces
    @ApplicationScoped
    public TimebasedIdentifierGenerator idGenerator(@PropertyValue(name = "TimebasedIdentifierGenerator.DeploymentContext") Integer deploymentContext) {
        return new TimebasedIdentifierGenerator(deploymentContext);
    }

    @Produces
    @PropertyValue(name = "")
    public String stringPropertyValue(@ApplicationProperties ApplicationConfig applicationConfig, InjectionPoint injectionPoint) {
        final PropertyValue property = injectionPoint.getAnnotated().getAnnotation(PropertyValue.class);
        return applicationConfig.getStringPropertyValue(property.name());
    }

    @Produces
    @PropertyValue(name = "")
    public Integer integerPropertyValue(@ApplicationProperties ApplicationConfig applicationConfig, InjectionPoint injectionPoint) {
        final PropertyValue property = injectionPoint.getAnnotated().getAnnotation(PropertyValue.class);
        return applicationConfig.getIntegerPropertyValue(property.name());
    }

    @ApplicationScoped
    @ApplicationProperties
    @Produces
    public ApplicationConfig applicationConfig() throws IOException {
        return new ApplicationConfigProvider()
                .fromDefaultResource()
                .fromEnvVar("JACS2_CONFIG")
                .fromMap(ApplicationConfigProvider.applicationArgs())
                .build();
    }

    @JacsDefault
    @ApplicationScoped
    @Produces
    public ServiceComputationQueue createTaskQueue(ExecutorService taskExecutor, @TaskQueuePoll ExecutorService queueInspector) {
        return new ServiceComputationQueue(taskExecutor, queueInspector);
    }

    @JacsDefault
    @ApplicationScoped
    @Produces
    public ServiceComputationFactory createComputationFactory(@JacsDefault ServiceComputationQueue computationQueue, Logger logger) {
        return new ServiceComputationFactory(computationQueue, logger);
    }

//    @ApplicationScoped
//    @Produces
//    public JacsServiceQueue createJacsServiceQueue(JacsServiceDataPersistence jacsServiceDataPersistence,
//                                                   @PropertyValue(name = "service.queue.id") String queueId,
//                                                   @PropertyValue(name = "service.queue.MaxCapacity") int maxReadyCapacity,
//                                                   Logger logger) {
//        return new InMemoryJacsServiceQueue(jacsServiceDataPersistence, queueId, maxReadyCapacity, logger);
//    }
}
