package org.janelia.jacs2.dao.mongo.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.janelia.it.jacs.model.domain.DomainObject;
import org.janelia.it.jacs.model.domain.Subject;
import org.janelia.it.jacs.model.domain.sample.AnatomicalArea;
import org.janelia.it.jacs.model.domain.sample.ObjectiveSample;
import org.janelia.it.jacs.model.domain.sample.SamplePipelineRun;
import org.janelia.it.jacs.model.domain.sample.SampleTile;
import org.janelia.it.jacs.model.domain.sample.TileLsmPair;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceEvent;

import java.io.IOException;
import java.io.UncheckedIOException;

public class DomainCodecProvider implements CodecProvider {

    private final ObjectMapper objectMapper;

    public DomainCodecProvider(ObjectMapperFactory objectMapperFactory) {
        this.objectMapper = objectMapperFactory.newMongoCompatibleObjectMapper();
    }

    @Override
    public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
        if (checkCodecApplicability(clazz)) {
            final Codec<Document> rawBsonDocumentCodec = registry.get(Document.class);
            return new Codec<T>() {
                @Override
                public T decode(BsonReader reader, DecoderContext decoderContext) {
                    try {
                        Document document = rawBsonDocumentCodec.decode(reader, decoderContext);
                        return objectMapper.readValue(document.toJson(), clazz);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }

                @Override
                public void encode(BsonWriter writer, T value, EncoderContext encoderContext) {
                    try {
                        String json = objectMapper.writeValueAsString(value);
                        rawBsonDocumentCodec.encode(writer, Document.parse(json), encoderContext);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }

                @Override
                public Class<T> getEncoderClass() {
                    return clazz;
                }
            };
        }
        return null;
    }

    private <T> boolean checkCodecApplicability(Class<T> clazz) {
        return DomainObject.class.isAssignableFrom(clazz)
                || ImmutableSet.of(ObjectiveSample.class, AnatomicalArea.class, SampleTile.class, TileLsmPair.class, SamplePipelineRun.class).contains(clazz)
                || Subject.class.equals(clazz)
                || JacsServiceData.class.equals(clazz)
                || JacsServiceEvent.class.equals(clazz);
    }
}
