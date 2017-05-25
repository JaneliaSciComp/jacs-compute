package org.janelia.jacs2.dao.mongo.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.janelia.jacs2.cdi.ObjectMapperFactory;

import java.io.IOException;
import java.io.UncheckedIOException;

public class JsonNodeCodec implements Codec<JsonNode> {

    private final ObjectMapper objectMapper;
    final Codec<Document> rawBsonDocumentCodec;

    public JsonNodeCodec(ObjectMapperFactory objectMapperFactory, CodecRegistry registry) {
        this.objectMapper = objectMapperFactory.newMongoCompatibleObjectMapper();
        this.rawBsonDocumentCodec = registry.get(Document.class);
    }

    @Override
    public JsonNode decode(BsonReader reader, DecoderContext decoderContext) {
        try {
            Document document = rawBsonDocumentCodec.decode(reader, decoderContext);
            return objectMapper.readTree(document.toJson());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void encode(BsonWriter writer, JsonNode value, EncoderContext encoderContext) {
        try {
            String json = objectMapper.writeValueAsString(value);
            rawBsonDocumentCodec.encode(writer, Document.parse(json), encoderContext);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Class<JsonNode> getEncoderClass() {
        return JsonNode.class;
    }
}
