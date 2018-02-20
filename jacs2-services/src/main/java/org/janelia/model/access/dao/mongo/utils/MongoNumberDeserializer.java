package org.janelia.model.access.dao.mongo.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.regex.Pattern;

public class MongoNumberDeserializer extends JsonDeserializer<Number> {

    Pattern FLOATING_POINT_PATTERN = Pattern.compile("^[-+]?[0-9]*\\.[0-9]+([eE][-+]?[0-9]+)?$");

    @Override
    public Number deserialize(JsonParser jsonParser, DeserializationContext ctxt) throws IOException {
        JsonNode node = jsonParser.readValueAsTree();
        if (node.get("$numberLong") != null) {
            return new BigInteger(node.get("$numberLong").asText());
        } else {
            String value = node.asText();
            if (FLOATING_POINT_PATTERN.matcher(value).matches()) {
                return new Double(value);
            } else {
                return new BigInteger(node.asText());
            }
        }
    }
}
