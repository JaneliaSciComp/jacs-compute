package org.janelia.jacs2.app;

import org.glassfish.jersey.server.ResourceConfig;

public class JAXAppConfig extends ResourceConfig {
    public JAXAppConfig(String... packageNames) {
        packages(true, packageNames);
    }
}
