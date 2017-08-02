package org.janelia.jacs2.dao.mongo.utils;

import org.apache.commons.lang3.StringUtils;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.janelia.it.jacs.model.domain.Reference;

/**
 * ReferenceCodec implements a Codec for a Reference type.
 */
public class ReferenceCodec implements Codec<Reference> {

    @Override
    public Reference decode(BsonReader reader, DecoderContext decoderContext) {
        String value = reader.readString();
        if (StringUtils.isBlank(value)) {
            return null;
        } else {
            return Reference.createFor(value);
        }
    }

    @Override
    public void encode(BsonWriter writer, Reference value, EncoderContext encoderContext) {
        if (value == null) {
            writer.writeNull();
        } else {
            writer.writeString(value.toString());
        }
    }

    @Override
    public Class<Reference> getEncoderClass() {
        return Reference.class;
    }
}
