package org.janelia.jacs2.testrest;

import org.janelia.jacs2.asyncservice.JacsServiceDataManager;
import org.janelia.jacs2.asyncservice.JacsServiceEngine;
import org.janelia.jacs2.asyncservice.ServiceRegistry;
import org.janelia.jacs2.auth.JWTProvider;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.dataservice.cronservice.CronScheduledServiceManager;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.inject.Produces;

import static org.mockito.Mockito.mock;

public class TestResourceDependenciesProducer {

    private Logger logger = LoggerFactory.getLogger(TestResourceDependenciesProducer.class);
    private JacsServiceDataManager jacsServiceDataManager = mock(JacsServiceDataManager.class);
    private ServiceRegistry serviceRegistry = mock(ServiceRegistry.class);
    private LegacyDomainDao legacyDomainDao = mock(LegacyDomainDao.class);
    private JWTProvider jwtProvider = mock(JWTProvider.class);
    private ObjectMapperFactory objectMapperFactory = ObjectMapperFactory.instance();
    private CronScheduledServiceManager jacsScheduledServiceDataManager = mock(CronScheduledServiceManager.class);
    private JacsServiceEngine jacsServiceEngine = mock(JacsServiceEngine.class);

    @Produces
    public Logger getLogger() {
        return logger;
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
    public LegacyDomainDao getLegacyDomainDao() {
        return legacyDomainDao;
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
