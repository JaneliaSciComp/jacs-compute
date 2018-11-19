package org.janelia.jacs2.app;

import org.glassfish.jersey.server.ResourceConfig;
import org.janelia.jacs2.filter.AuthFilter;
import org.janelia.jacs2.filter.CORSResponseFilter;
import org.janelia.jacs2.filter.JwtDecoder;
import org.janelia.jacs2.provider.ObjectMapperResolver;
import org.janelia.jacs2.rest.InvalidArgumentRequestHandler;
import org.janelia.jacs2.rest.InvalidJsonRequestHandler;
import org.janelia.jacs2.rest.v2.AppVersionResource;

public class JAXAppConfig extends ResourceConfig {
    JAXAppConfig(String... packageNames) {
        packages(true, packageNames);
        registerClasses(
//                com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider.class,
                InvalidArgumentRequestHandler.class,
                InvalidJsonRequestHandler.class,
                ObjectMapperResolver.class,
                // Putting the multipart package in the param above does not work. We need to be explicit with the classname.
                org.glassfish.jersey.media.multipart.MultiPartFeature.class,
                AppVersionResource.class,
                CORSResponseFilter.class,
                AuthFilter.class);

    }
}
