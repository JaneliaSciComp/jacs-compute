package org.janelia.jacs2.dao.mongo.utils;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.janelia.jacs2.cdi.ObjectMapperFactory;

public class JacksonCodecProvider implements CodecProvider {

    private final ObjectMapperFactory objectMapperFactory;

    public JacksonCodecProvider(final ObjectMapperFactory objectMapperFactory) {
        this.objectMapperFactory = objectMapperFactory;
    }

    @Override
    public <T> Codec<T> get(final Class<T> type, final CodecRegistry registry) {
        return new JacksonCodec<>(objectMapperFactory, registry, type);
    }
}
