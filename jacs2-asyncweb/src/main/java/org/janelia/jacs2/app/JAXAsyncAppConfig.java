package org.janelia.jacs2.app;

public class JAXAsyncAppConfig extends JAXAppConfig {
    public JAXAsyncAppConfig() {
        super("io.swagger.jaxrs.listing",
              "org.janelia.jacs2.rest.async.v2",
              "org.janelia.jacs2.job",
              "org.janelia.jacs2.provider");
    }
}
