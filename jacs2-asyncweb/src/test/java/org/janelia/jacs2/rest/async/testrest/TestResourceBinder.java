package org.janelia.jacs2.rest.async.testrest;

import java.lang.annotation.Annotation;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.glassfish.hk2.api.Injectee;
import org.glassfish.hk2.api.InjectionResolver;
import org.glassfish.hk2.api.ServiceHandle;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.janelia.jacs2.asyncservice.JacsServiceDataManager;
import org.janelia.jacs2.asyncservice.JacsServiceEngine;
import org.janelia.jacs2.asyncservice.ServiceRegistry;
import org.janelia.jacs2.auth.JWTProvider;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.cronservice.CronScheduledServiceManager;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.SubjectDao;
import org.slf4j.Logger;

public class TestResourceBinder extends AbstractBinder {

    static class PropertyResolver implements InjectionResolver<PropertyValue> {

        @Inject
        ApplicationConfig applicationConfig;


        @Override
        public Object resolve(Injectee injectee, ServiceHandle<?> root) {
            return injectee.getRequiredQualifiers().stream().filter(a -> a.annotationType().equals(PropertyValue.class))
                    .findFirst()
                    .map(a -> (PropertyValue) a)
                    .map(pv -> applicationConfig.getStringPropertyValue(pv.name()))
                    .orElseThrow(() -> new IllegalStateException("PropertyValue not found"))
                    ;
        }

        @Override
        public boolean isConstructorParameterIndicator() {
            return false;
        }

        @Override
        public boolean isMethodParameterIndicator() {
            return false;
        }
    };

    private final TestResourceDependenciesProducer dependenciesProducer;

    @Inject
    public TestResourceBinder(TestResourceDependenciesProducer dependenciesProducer) {
        this.dependenciesProducer = dependenciesProducer;
    }

    @Override
    protected void configure() {
        ApplicationProperties applicationPropertiesAnnotation = getAnnotation(ApplicationProperties.class, "getApplicationConfig");

        bind(dependenciesProducer.getLogger()).to(Logger.class);
        bind(dependenciesProducer.getApplicationConfig()).to(ApplicationConfig.class).qualifiedBy(applicationPropertiesAnnotation);
        bind(dependenciesProducer.getJacsServiceDataManager()).to(JacsServiceDataManager.class);
        bind(dependenciesProducer.getServiceRegistry()).to(ServiceRegistry.class);
        bind(dependenciesProducer.getSubjectDao()).to(SubjectDao.class);
        bind(dependenciesProducer.getLegacyDomainDao()).to(LegacyDomainDao.class);
        bind(dependenciesProducer.getObjectMapperFactory()).to(ObjectMapperFactory.class);
        bind(dependenciesProducer.getJacsScheduledServiceDataManager()).to(CronScheduledServiceManager.class);
        bind(dependenciesProducer.getJacsServiceEngine()).to(JacsServiceEngine.class);
        bind(dependenciesProducer.getJWTProvider()).to(JWTProvider.class);
        bind(PropertyResolver.class)
                .to(new TypeLiteral<InjectionResolver<PropertyValue>>() {})
                .in(Singleton.class);
    }

    private <A extends Annotation> A getAnnotation(Class<A> annotationClass, String exampleMethodName, Class<?>... exampleMethodParameterTypes) {
        try {
            return dependenciesProducer.getClass().getMethod(exampleMethodName, exampleMethodParameterTypes).getAnnotation(annotationClass);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

}
