package org.janelia.jacs2.rest.async.v2;

import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;

import org.glassfish.jersey.ext.cdi1x.internal.CdiComponentProvider;
import org.glassfish.jersey.inject.hk2.Hk2InjectionManagerFactory;
import org.glassfish.jersey.test.JerseyTest;
import org.janelia.jacs2.app.JAXAsyncAppConfig;
import org.janelia.jacs2.rest.async.testrest.TestResourceDependenciesProducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;

public class AbstractAsyncServicesAppResourceTest extends JerseyTest {

    protected SeContainer container;
    protected TestResourceDependenciesProducer dependenciesProducer;

    @Override
    protected JAXAsyncAppConfig configure() {
        return new JAXAsyncAppConfig();
    }

    @BeforeEach
    public void setupPreconditionCheck() {
        Assumptions.assumeTrue(Hk2InjectionManagerFactory.isImmediateStrategy());
    }

    @BeforeEach
    public void setUp() throws Exception {
        dependenciesProducer = new TestResourceDependenciesProducer();
        SeContainerInitializer containerInit = SeContainerInitializer
                .newInstance()
                .disableDiscovery()
                .addExtensions(new CdiComponentProvider())
                .addBeanClasses(
                        ServiceInfoResource.class,
                        TestResourceDependenciesProducer.class
                );
        container = containerInit.initialize();
        super.setUp();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (container != null) {
            container.close();
        }
        super.tearDown();
    }

}
