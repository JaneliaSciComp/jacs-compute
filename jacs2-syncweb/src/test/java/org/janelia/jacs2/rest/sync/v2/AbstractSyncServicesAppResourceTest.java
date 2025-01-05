package org.janelia.jacs2.rest.sync.v2;

import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;

import org.glassfish.jersey.ext.cdi1x.internal.CdiComponentProvider;
import org.glassfish.jersey.inject.hk2.Hk2InjectionManagerFactory;
import org.glassfish.jersey.test.JerseyTest;
import org.janelia.jacs2.app.JAXSyncAppConfig;
import org.janelia.jacs2.rest.sync.testrest.TestResourceDependenciesProducer;
import org.janelia.jacs2.rest.sync.v2.dataresources.DatasetResource;
import org.janelia.jacs2.rest.sync.v2.dataresources.UserResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;

public class AbstractSyncServicesAppResourceTest extends JerseyTest {

    protected SeContainer container;
    protected TestResourceDependenciesProducer dependenciesProducer;

    @Override
    protected JAXSyncAppConfig configure() {
        return new JAXSyncAppConfig();
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
                        DatasetResource.class,
                        UserResource.class,
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
