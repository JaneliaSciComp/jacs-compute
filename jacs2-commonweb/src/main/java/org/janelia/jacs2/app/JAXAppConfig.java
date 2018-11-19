package org.janelia.jacs2.app;

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.filter.AuthFilter;
import org.janelia.jacs2.filter.CORSResponseFilter;
import org.janelia.jacs2.provider.ObjectMapperResolver;
import org.janelia.jacs2.rest.InvalidArgumentRequestHandler;
import org.janelia.jacs2.rest.InvalidJsonRequestHandler;
import org.janelia.jacs2.rest.v2.AppVersionResource;
import org.janelia.model.NumberSerializerModule;

public class JAXAppConfig extends ResourceConfig {
    JAXAppConfig(String... packageNames) {
        register(new JacksonJaxbJsonProvider(ObjectMapperFactory.instance()
                .newObjectMapper().registerModule(new NumberSerializerModule()), JacksonJaxbJsonProvider.DEFAULT_ANNOTATIONS)
        );
        registerClasses(
                InvalidArgumentRequestHandler.class,
                InvalidJsonRequestHandler.class,
                ObjectMapperResolver.class,
                // Putting the multipart package in the param above does not work. We need to be explicit with the classname.
                MultiPartFeature.class,
                AppVersionResource.class,
                CORSResponseFilter.class,
                AuthFilter.class);
        packages(true, packageNames);
    }
}
