package org.janelia.jacs2.cdi;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.janelia.jacs2.cdi.qualifier.*;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.model.access.dao.mongo.utils.TimebasedIdentifierGenerator;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;
import java.io.IOException;

@ApplicationScoped
public class ApplicationProducer {

    @Produces
    public ObjectMapper objectMapper(ObjectMapperFactory objectMapperFactory) {
        return objectMapperFactory.getDefaultObjectMapper();
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

    @Produces
    @IntPropertyValue(name = "")
    public int intPropertyValueWithDefault(@ApplicationProperties ApplicationConfig applicationConfig, InjectionPoint injectionPoint) {
        final IntPropertyValue property = injectionPoint.getAnnotated().getAnnotation(IntPropertyValue.class);
        return applicationConfig.getIntegerPropertyValue(property.name(), property.defaultValue());
    }

    @Produces
    @StrPropertyValue(name = "")
    public String stringPropertyValueWithDefault(@ApplicationProperties ApplicationConfig applicationConfig, InjectionPoint injectionPoint) {
        final StrPropertyValue property = injectionPoint.getAnnotated().getAnnotation(StrPropertyValue.class);
        return applicationConfig.getStringPropertyValue(property.name(), property.defaultValue());
    }

    @Produces
    @BoolPropertyValue(name = "")
    public boolean booleanPropertyValueWithDefault(@ApplicationProperties ApplicationConfig applicationConfig, InjectionPoint injectionPoint) {
        final BoolPropertyValue property = injectionPoint.getAnnotated().getAnnotation(BoolPropertyValue.class);
        return applicationConfig.getBooleanPropertyValue(property.name(), property.defaultValue());
    }

    @ApplicationScoped
    @ApplicationProperties
    @Produces
    public ApplicationConfig applicationConfig() throws IOException {
        return new ApplicationConfigProvider()
                .fromDefaultResources()
                .fromEnvVar("JACS2_CONFIG")
                .fromMap(ApplicationConfigProvider.applicationArgs())
                .build();
    }
}
