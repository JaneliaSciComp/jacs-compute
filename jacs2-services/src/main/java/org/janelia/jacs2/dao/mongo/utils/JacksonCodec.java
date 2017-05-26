package org.janelia.jacs2.dao.mongo.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.RawBsonDocument;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.janelia.jacs2.cdi.ObjectMapperFactory;

import java.io.IOException;
import java.io.UncheckedIOException;

public class JacksonCodec<T> implements Codec<T> {

    private final ObjectMapper objectMapper;
    private final Codec<RawBsonDocument> rawBsonDocumentCodec;
    private final Class<T> type;


    public JacksonCodec(ObjectMapperFactory objectMapperFactory, CodecRegistry registry, Class<T> type) {
        this.objectMapper = objectMapperFactory.newMongoCompatibleObjectMapper();
        this.rawBsonDocumentCodec = registry.get(RawBsonDocument.class);
        this.type = type;
    }

    @Override
    public T decode(BsonReader reader, DecoderContext decoderContext) {
        try {
            RawBsonDocument document = rawBsonDocumentCodec.decode(reader, decoderContext);
            return objectMapper.readValue(document.getByteBuffer().array(), type);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void encode(BsonWriter writer, T value, EncoderContext encoderContext) {
        try {
            byte[] data = objectMapper.writeValueAsBytes(value);
            rawBsonDocumentCodec.encode(writer, new RawBsonDocument(data), encoderContext);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Class<T> getEncoderClass() {
        return type;
    }
}
