package org.janelia.jacs2.app;

import com.fasterxml.jackson.jaxrs.xml.JacksonJaxbXMLProvider;

public class JAXSyncAppConfig extends JAXAppConfig {
    public JAXSyncAppConfig() {
        super("io.swagger.jaxrs.listing",
                "org.janelia.jacs2.rest.sync.v2",
                "org.janelia.jacs2.provider");
        register(JacksonJaxbXMLProvider.class);
    }
}
