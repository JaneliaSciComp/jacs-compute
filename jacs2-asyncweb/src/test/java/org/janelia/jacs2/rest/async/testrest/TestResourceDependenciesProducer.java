package org.janelia.jacs2.rest.async.testrest;

import org.janelia.jacs2.asyncservice.JacsServiceDataManager;
import org.janelia.jacs2.asyncservice.JacsServiceEngine;
import org.janelia.jacs2.asyncservice.ServiceRegistry;
import org.janelia.jacs2.auth.JWTProvider;
import org.janelia.jacs2.cdi.ApplicationConfigProvider;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.cronservice.CronScheduledServiceManager;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.SubjectDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.spi.InjectionPoint;

import com.google.common.collect.ImmutableMap;

import static org.mockito.Mockito.mock;

/**
 * This class is responsible for creating mock objects to be injected  in the test resources.
 */
public class TestResourceDependenciesProducer {

    private static JacsServiceDataManager jacsServiceDataManager = mock(JacsServiceDataManager.class);
    private static ServiceRegistry serviceRegistry = mock(ServiceRegistry.class);
    private static SubjectDao subjectDao = mock(SubjectDao.class);
    private static JWTProvider jwtProvider = mock(JWTProvider.class);
    private static ObjectMapperFactory objectMapperFactory = ObjectMapperFactory.instance();
    private static CronScheduledServiceManager jacsScheduledServiceDataManager = mock(CronScheduledServiceManager.class);
    private static JacsServiceEngine jacsServiceEngine = mock(JacsServiceEngine.class);

    @ApplicationProperties
    @Produces
    public ApplicationConfig getApplicationConfig() {
        return new ApplicationConfigProvider()
                .fromMap(ImmutableMap.<String, String>builder()
                        .put("JACS.ApiKey", "TESTKEY")
                        .put("JACS.SystemAppUserName", "TESTUSER")
                        .build()
                )
                .build();
    }

    @PropertyValue(name = "")
    @Produces
    public String getStringPropertyValue(@ApplicationProperties ApplicationConfig applicationConfig, InjectionPoint injectionPoint) {
        final PropertyValue property = injectionPoint.getAnnotated().getAnnotation(PropertyValue.class);
        return applicationConfig.getStringPropertyValue(property.name());
    }

    @Produces
    public JacsServiceDataManager getJacsServiceDataManager() {
        return jacsServiceDataManager;
    }

    @Produces
    public ServiceRegistry getServiceRegistry() {
        return serviceRegistry;
    }

    @Produces
    public SubjectDao getSubjectDao() {
        return subjectDao;
    }

    @Produces
    public ObjectMapperFactory getObjectMapperFactory() {
        return objectMapperFactory;
    }

    @Produces
    public CronScheduledServiceManager getJacsScheduledServiceDataManager() {
        return jacsScheduledServiceDataManager;
    }

    @Produces
    public JacsServiceEngine getJacsServiceEngine() {
        return jacsServiceEngine;
    }

    @Produces
    public JWTProvider getJWTProvider() {
        return jwtProvider;
    }
}
