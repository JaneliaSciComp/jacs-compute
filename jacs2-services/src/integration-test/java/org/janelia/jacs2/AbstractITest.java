package org.janelia.jacs2;

import com.google.common.collect.ImmutableMap;
import org.janelia.jacs2.cdi.ApplicationConfigProvider;
import org.janelia.jacs2.config.ApplicationConfig;
import org.junit.BeforeClass;

import java.util.Properties;

public abstract class AbstractITest {
    protected static ApplicationConfig integrationTestsConfig;

    @BeforeClass
    public static void setUpTestsConfig() {
        integrationTestsConfig = new ApplicationConfigProvider()
                .fromDefaultResources()
                .fromFile("src/integration-test/resources/jacs_test.properties")
                .fromEnvVar("JACS2_CONFIG_TEST")
                .build();
    }

}
