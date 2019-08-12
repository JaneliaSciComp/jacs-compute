package org.janelia.model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;
import java.math.BigInteger;

public class NumberSerializerModule extends SimpleModule {

    static class NumberSerializer extends JsonSerializer<Number> {

        @Override
        public void serialize(Number value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException {
            if (value == null) {
                jgen.writeNull();
            } else if (value instanceof Long || value instanceof BigInteger) {
                jgen.writeString(value.toString());
            } else {
                jgen.writeNumber(value.toString());
            }
        }
    }

    public NumberSerializerModule() {
        addSerializer(Number.class, new NumberSerializer());
    }
}
