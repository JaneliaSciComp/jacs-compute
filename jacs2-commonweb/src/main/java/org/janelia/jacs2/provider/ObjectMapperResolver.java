package org.janelia.jacs2.provider;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.model.NumberSerializerModule;

import javax.inject.Inject;
import javax.ws.rs.ext.ContextResolver;

public class ObjectMapperResolver implements ContextResolver<ObjectMapper> {

    private final ObjectMapper mapper;

    @Inject
    public ObjectMapperResolver(ObjectMapperFactory mapperFactory) {
        this.mapper = mapperFactory.newObjectMapper()
                .registerModule(new NumberSerializerModule());
    }

    @Override
    public ObjectMapper getContext(Class<?> type) {
        return mapper;
    }
}
