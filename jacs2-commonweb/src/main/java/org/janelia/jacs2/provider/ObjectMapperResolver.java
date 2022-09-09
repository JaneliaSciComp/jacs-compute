package org.janelia.jacs2.provider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.model.NumberSerializerModule;

import javax.inject.Inject;
import javax.ws.rs.ext.ContextResolver;

public class ObjectMapperResolver implements ContextResolver<ObjectMapper> {

    private final ObjectMapper mapper;

    @Inject
    public ObjectMapperResolver(ObjectMapperFactory mapperFactory) {
        this.mapper = mapperFactory.newObjectMapper()
                .registerModule(new NumberSerializerModule())
                .configure(SerializationFeature.FAIL_ON_UNWRAPPED_TYPE_IDENTIFIERS, false);
    }

    @Override
    public ObjectMapper getContext(Class<?> type) {
        return mapper;
    }
}
