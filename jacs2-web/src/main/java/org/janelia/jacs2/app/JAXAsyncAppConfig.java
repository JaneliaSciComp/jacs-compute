package org.janelia.jacs2.app;

import org.glassfish.jersey.server.ResourceConfig;

public class JAXAsyncAppConfig extends ResourceConfig {
    public JAXAsyncAppConfig() {
        packages(true,
                "org.janelia.jacs2.rest.async",
                "org.janelia.jacs2.job",
                "org.janelia.jacs2.provider");
    }
}
