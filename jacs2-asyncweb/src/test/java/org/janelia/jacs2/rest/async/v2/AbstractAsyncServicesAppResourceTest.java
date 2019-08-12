package org.janelia.jacs2.rest.async.v2;

import org.janelia.jacs2.app.JAXAsyncAppConfig;
import org.janelia.jacs2.rest.async.testrest.TestResourceBinder;
import org.janelia.jacs2.rest.async.testrest.TestResourceDependenciesProducer;
import org.janelia.jacs2.rest.testrest.AbstractCdiInjectedResourceTest;

public class AbstractAsyncServicesAppResourceTest extends AbstractCdiInjectedResourceTest {

    TestResourceDependenciesProducer dependenciesProducer;

    @Override
    protected JAXAsyncAppConfig configure() {
        dependenciesProducer = new TestResourceDependenciesProducer();
        JAXAsyncAppConfig appConfig = new JAXAsyncAppConfig();
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
