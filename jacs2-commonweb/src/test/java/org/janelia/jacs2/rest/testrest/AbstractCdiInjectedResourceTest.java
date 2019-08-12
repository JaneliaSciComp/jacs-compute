package org.janelia.jacs2.rest.testrest;

import org.glassfish.jersey.ext.cdi1x.internal.CdiComponentProvider;
import org.glassfish.jersey.inject.hk2.Hk2InjectionManagerFactory;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;

import static org.mockito.Mockito.spy;

public abstract class AbstractCdiInjectedResourceTest extends JerseyTest {

    private SeContainer container;

    @Before
    public void setupPreconditionCheck() {
        Assume.assumeTrue(Hk2InjectionManagerFactory.isImmediateStrategy());
    }

    @Before
    public void setUp() throws Exception {
        SeContainerInitializer containerInit = SeContainerInitializer
                .newInstance()
                .disableDiscovery()
                .addExtensions(new CdiComponentProvider())
                .addBeanClasses(getTestBeanProviders())
                ;
        container = spy(containerInit.initialize());
        super.setUp();
    }

    protected Class<?>[] getTestBeanProviders() {
        return new Class<?>[0];
    }

    @After
    public void tearDown() throws Exception {
        container.close();
        super.tearDown();
    }

}
