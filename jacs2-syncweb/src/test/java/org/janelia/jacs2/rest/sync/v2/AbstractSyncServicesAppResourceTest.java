package org.janelia.jacs2.rest.sync.v2;

import org.janelia.jacs2.app.JAXSyncAppConfig;
import org.janelia.jacs2.rest.sync.testrest.TestResourceBinder;
import org.janelia.jacs2.rest.sync.testrest.TestResourceDependenciesProducer;
import org.janelia.jacs2.rest.testrest.AbstractCdiInjectedResourceTest;

public class AbstractSyncServicesAppResourceTest extends AbstractCdiInjectedResourceTest {

    TestResourceDependenciesProducer dependenciesProducer;

    @Override
    protected JAXSyncAppConfig configure() {
        dependenciesProducer = new TestResourceDependenciesProducer();
        JAXSyncAppConfig appConfig = new JAXSyncAppConfig();
        appConfig.register(new TestResourceBinder(dependenciesProducer));
        return appConfig;
    }

    @Override
    protected Class<?>[] getTestBeanProviders() {
        return new Class<?>[] {
                TestResourceDependenciesProducer.class
        };
    }

}
