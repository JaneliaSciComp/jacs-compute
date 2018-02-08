package org.janelia.jacs2.app;

import org.glassfish.jersey.server.ResourceConfig;
import org.janelia.jacs2.filter.AuthFilter;

public class JAXSyncAppConfig extends ResourceConfig {
    public JAXSyncAppConfig() {
        packages(true,
                "io.swagger.jaxrs.listing",
                "org.janelia.jacs2.rest.sync.v2",
                "org.janelia.jacs2.provider");
        registerClasses(
                com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider.class,
                // Putting the multipart package in the param above does not work. We need to be explicit with the classname.
                org.glassfish.jersey.media.multipart.MultiPartFeature.class,
                AuthFilter.class);
        // Disable WADL generation, because uses JAXB, produces XML, and ignores our Jackson annotations, and generally doesn't work correctly
        property("jersey.config.server.wadl.disableWadl", "true");
    }
}
