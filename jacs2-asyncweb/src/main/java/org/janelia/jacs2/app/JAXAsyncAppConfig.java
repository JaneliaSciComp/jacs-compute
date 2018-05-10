package org.janelia.jacs2.app;

public class JAXAsyncAppConfig extends JAXAppConfig {
    public JAXAsyncAppConfig() {
        super("io.swagger.jaxrs.listing",
              "org.janelia.jacs2.rest.async.v2",
              "org.janelia.jacs2.job",
              "org.janelia.jacs2.provider");
        // Disable WADL generation, because uses JAXB, produces XML, and ignores our Jackson annotations, and generally doesn't work correctly
        property("jersey.config.server.wadl.disableWadl", "true");
    }
}
