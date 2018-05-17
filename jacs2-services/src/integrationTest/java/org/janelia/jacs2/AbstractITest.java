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
                .fromMap(ImmutableMap.of(
                        "MongoDB.ConnectionURL", "mongodb://localhost:27017",
                        "MongoDB.Database", "{user.name}_jacs_test"
                ))
                .fromFile("src/integrationTest/resources/jacs_test.properties")
                .fromEnvVar("JACS2_CONFIG_TEST")
                .build();
    }

}
