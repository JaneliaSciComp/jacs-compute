package org.janelia.jacs2.app;

import org.glassfish.jersey.server.ResourceConfig;

public class JAXSyncAppConfig extends ResourceConfig {
    public JAXSyncAppConfig() {
        packages(true,
                "org.janelia.jacs2.rest.sync",
                "org.janelia.jacs2.provider");
    }
}
