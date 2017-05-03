package org.janelia.jacs2.app;

public class JAXAsyncAppConfig extends JAXAppConfig {
    public JAXAsyncAppConfig() {
        super("org.janelia.jacs2.rest.async",
              "org.janelia.jacs2.job",
              "org.janelia.jacs2.provider");
    }
}
