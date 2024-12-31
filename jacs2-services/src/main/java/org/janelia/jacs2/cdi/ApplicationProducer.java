package org.janelia.jacs2.cdi;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.spi.InjectionPoint;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.BoolPropertyValue;
import org.janelia.jacs2.cdi.qualifier.DoublePropertyValue;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.cdi.qualifier.StrPropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.model.access.cdi.DaoObjectMapper;
import org.janelia.model.access.domain.IdGenerator;
import org.janelia.model.access.domain.TimebasedIdentifierGenerator;

@ApplicationScoped
public class ApplicationProducer {

    @Produces
    public ObjectMapper objectMapper(ObjectMapperFactory objectMapperFactory) {
        return objectMapperFactory.getDefaultObjectMapper();
    }

    @DaoObjectMapper
    @Produces
    public ObjectMapper mongoObjectMapper(ObjectMapperFactory objectMapperFactory) {
        return objectMapperFactory.newMongoCompatibleObjectMapper();
    }

    @JacsDefault
    @ApplicationScoped
    @Produces
    public IdGenerator<Long> idGenerator(@PropertyValue(name = "TimebasedIdentifierGenerator.DeploymentContext") Integer deploymentContext) {
        return new TimebasedIdentifierGenerator(deploymentContext);
    }

    @PropertyValue(name = "")
    @Produces
    public String stringPropertyValue(@ApplicationProperties ApplicationConfig applicationConfig, InjectionPoint injectionPoint) {
        final PropertyValue property = injectionPoint.getAnnotated().getAnnotation(PropertyValue.class);
        return applicationConfig.getStringPropertyValue(property.name());
    }

    @PropertyValue(name = "")
    @Produces
    public Integer integerPropertyValue(@ApplicationProperties ApplicationConfig applicationConfig, InjectionPoint injectionPoint) {
        final PropertyValue property = injectionPoint.getAnnotated().getAnnotation(PropertyValue.class);
        return applicationConfig.getIntegerPropertyValue(property.name());
    }

    @DoublePropertyValue(name = "")
    @Produces
    public double doublePropertyValueWithDefault(@ApplicationProperties ApplicationConfig applicationConfig, InjectionPoint injectionPoint) {
        final DoublePropertyValue property = injectionPoint.getAnnotated().getAnnotation(DoublePropertyValue.class);
        return applicationConfig.getDoublePropertyValue(property.name(), property.defaultValue());
    }

    @IntPropertyValue(name = "")
    @Produces
    public int intPropertyValueWithDefault(@ApplicationProperties ApplicationConfig applicationConfig, InjectionPoint injectionPoint) {
        final IntPropertyValue property = injectionPoint.getAnnotated().getAnnotation(IntPropertyValue.class);
        return applicationConfig.getIntegerPropertyValue(property.name(), property.defaultValue());
    }

    @StrPropertyValue(name = "")
    @Produces
    public String stringPropertyValueWithDefault(@ApplicationProperties ApplicationConfig applicationConfig, InjectionPoint injectionPoint) {
        final StrPropertyValue property = injectionPoint.getAnnotated().getAnnotation(StrPropertyValue.class);
        return applicationConfig.getStringPropertyValue(property.name(), property.defaultValue());
    }

    @BoolPropertyValue(name = "")
    @Produces
    public boolean booleanPropertyValueWithDefault(@ApplicationProperties ApplicationConfig applicationConfig, InjectionPoint injectionPoint) {
        final BoolPropertyValue property = injectionPoint.getAnnotated().getAnnotation(BoolPropertyValue.class);
        return applicationConfig.getBooleanPropertyValue(property.name(), property.defaultValue());
    }

    @ApplicationProperties
    @ApplicationScoped
    @Produces
    public ApplicationConfig applicationConfig() {
        return new ApplicationConfigProvider()
                .fromDefaultResources()
                .fromEnvVar("JACS2_CONFIG")
                .fromMap(ApplicationConfigProvider.getAppDynamicArgs())
                .build();
    }
}
