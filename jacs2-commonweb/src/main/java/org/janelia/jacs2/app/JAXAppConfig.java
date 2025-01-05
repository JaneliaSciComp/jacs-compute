package org.janelia.jacs2.app;

import com.fasterxml.jackson.jakarta.rs.json.JacksonXmlBindJsonProvider;
import com.fasterxml.jackson.jakarta.rs.xml.JacksonXmlBindXMLProvider;
import org.glassfish.jersey.server.ResourceConfig;
import org.janelia.jacs2.auth.JacsSecurityContext;
import org.janelia.jacs2.filter.AuthFilter;
import org.janelia.jacs2.filter.CORSResponseFilter;
import org.janelia.jacs2.provider.ObjectMapperResolver;
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
                ObjectMapperResolver.class,
                JacksonXmlBindJsonProvider.class,
                JacksonXmlBindXMLProvider.class,
                AppVersionResource.class,
                CORSResponseFilter.class,
                AuthFilter.class,
                JacsSecurityContext.class
        );
        // Disable WADL generation, because uses JAXB, produces XML, and ignores our Jackson annotations,
        // and generally doesn't work correctly
        property("jersey.config.server.wadl.disableWadl", "true");
    }
}
