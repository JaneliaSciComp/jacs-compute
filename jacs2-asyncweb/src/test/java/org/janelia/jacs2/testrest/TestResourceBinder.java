package org.janelia.jacs2.testrest;

import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.janelia.jacs2.asyncservice.JacsServiceDataManager;
import org.janelia.jacs2.asyncservice.JacsServiceEngine;
import org.janelia.jacs2.asyncservice.ServiceRegistry;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.dataservice.cronservice.CronScheduledServiceManager;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.slf4j.Logger;

import javax.inject.Inject;

public class TestResourceBinder extends AbstractBinder {
    private final TestResourceDependenciesProducer dependenciesProducer;

    @Inject
    public TestResourceBinder(TestResourceDependenciesProducer dependenciesProducer) {
        this.dependenciesProducer = dependenciesProducer;
    }

    @Override
    protected void configure() {
        bind(dependenciesProducer.getLogger()).to(Logger.class);
        bind(dependenciesProducer.getJacsServiceDataManager()).to(JacsServiceDataManager.class);
        bind(dependenciesProducer.getServiceRegistry()).to(ServiceRegistry.class);
        bind(dependenciesProducer.getLegacyDomainDao()).to(LegacyDomainDao.class);
        bind(dependenciesProducer.getObjectMapperFactory()).to(ObjectMapperFactory.class);
        bind(dependenciesProducer.getJacsScheduledServiceDataManager()).to(CronScheduledServiceManager.class);
        bind(dependenciesProducer.getJacsServiceEngine()).to(JacsServiceEngine.class);
    }
}
