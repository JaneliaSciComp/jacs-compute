package org.janelia.jacs2.app;

import org.glassfish.jersey.server.ResourceConfig;
import org.janelia.jacs2.filter.AuthFilter;
import org.janelia.jacs2.provider.ObjectMapperResolver;

public class JAXAppConfig extends ResourceConfig {
    JAXAppConfig(String... packageNames) {
        packages(true, packageNames);
        registerClasses(
                com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider.class,
                // Putting the multipart package in the param above does not work. We need to be explicit with the classname.
                org.glassfish.jersey.media.multipart.MultiPartFeature.class,
                ObjectMapperResolver.class,
                AuthFilter.class);

    }
}
