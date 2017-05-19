package org.janelia.jacs2.cdi;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dao.mongo.utils.TimebasedIdentifierGenerator;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.inject.Singleton;
import java.io.IOException;

public class ApplicationProducer {

    @Singleton
    @Produces
    public ObjectMapper objectMapper() {
        return ObjectMapperFactory.instance().getDefaultObjectMapper();
    }

    @Singleton
    @Produces
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

}
