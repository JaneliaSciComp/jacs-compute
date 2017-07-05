package org.janelia.jacs2;

import org.janelia.jacs2.cdi.ApplicationConfigProvider;
import org.junit.BeforeClass;

import java.util.Properties;

public abstract class AbstractITest {
    protected static Properties integrationTestsConfig;

    @BeforeClass
    public static void setUpTestsConfig() {
        integrationTestsConfig = new ApplicationConfigProvider()
                .fromFile("src/integration-test/resources/jacs_test.properties")
                .fromEnvVar("JACS2_CONFIG_TEST")
                .build();
    }

}
