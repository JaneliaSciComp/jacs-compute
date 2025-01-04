package org.janelia.jacs2.app;

import com.fasterxml.jackson.jakarta.rs.json.JacksonXmlBindJsonProvider;
import org.glassfish.jersey.server.ResourceConfig;
import org.janelia.jacs2.filter.CORSResponseFilter;
import org.janelia.jacs2.rest.IllegalStateRequestHandler;
import org.janelia.jacs2.rest.InvalidArgumentRequestHandler;
import org.janelia.jacs2.rest.InvalidJsonRequestHandler;
import org.janelia.jacs2.rest.v2.AppVersionResource;

public class JAXAppConfig extends ResourceConfig {
    protected JAXAppConfig(String... packageNames) {
        packages(true, packageNames);
        registerClasses(
                InvalidArgumentRequestHandler.class,
                IllegalStateRequestHandler.class,
                InvalidJsonRequestHandler.class,
                JacksonXmlBindJsonProvider.class,
                AppVersionResource.class,
                CORSResponseFilter.class);
        // Disable WADL generation, because uses JAXB, produces XML, and ignores our Jackson annotations,
        // and generally doesn't work correctly
        property("jersey.config.server.wadl.disableWadl", "true");
    }
}
