package org.janelia.jacs2.app;

public class JAXSyncAppConfig extends JAXAppConfig {
    JAXSyncAppConfig() {
        super("io.swagger.jaxrs.listing",
                "org.janelia.jacs2.rest.sync.v2",
                "org.janelia.jacs2.provider");
    }
}
