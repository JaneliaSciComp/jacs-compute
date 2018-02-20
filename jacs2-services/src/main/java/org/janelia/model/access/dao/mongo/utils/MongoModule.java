package org.janelia.model.access.dao.mongo.utils;

import com.fasterxml.jackson.databind.module.SimpleModule;

import java.util.Date;

public class MongoModule extends SimpleModule {

    public MongoModule() {
        addSerializer(Date.class, new ISODateSerializer());
        addDeserializer(Date.class, new ISODateDeserializer());
        addDeserializer(Long.class, new MongoNumberLongDeserializer());
        addDeserializer(Double.class, new MongoNumberDoubleDeserializer());
        addDeserializer(Number.class, new MongoNumberDeserializer());
    }
}
