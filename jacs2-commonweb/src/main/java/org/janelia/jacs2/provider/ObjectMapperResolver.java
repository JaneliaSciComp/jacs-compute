package org.janelia.jacs2.provider;

import jakarta.inject.Inject;
import jakarta.ws.rs.ext.ContextResolver;
import jakarta.ws.rs.ext.Provider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.module.jakarta.xmlbind.JakartaXmlBindAnnotationModule;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.model.NumberSerializerModule;

@Provider
public class ObjectMapperResolver implements ContextResolver<ObjectMapper> {

    private final ObjectMapper mapper;

    @Inject
    public ObjectMapperResolver(ObjectMapperFactory mapperFactory) {
        this.mapper = mapperFactory.newObjectMapper()
                .registerModule(new NumberSerializerModule())
                .registerModule(new JakartaXmlBindAnnotationModule())
                .configure(SerializationFeature.FAIL_ON_UNWRAPPED_TYPE_IDENTIFIERS, false);
    }

    @Override
    public ObjectMapper getContext(Class<?> type) {
        return mapper;
    }
}
