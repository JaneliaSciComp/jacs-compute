package org.janelia.jacs2.cdi;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker;
import com.fasterxml.jackson.datatype.joda.JodaModule;

import org.janelia.model.access.domain.dao.mongo.mongodbutils.MongoModule;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY;
import static com.fasterxml.jackson.databind.MapperFeature.AUTO_DETECT_GETTERS;
import static com.fasterxml.jackson.databind.MapperFeature.AUTO_DETECT_SETTERS;

public class ObjectMapperFactory {
    private static final ObjectMapperFactory INSTANCE = new ObjectMapperFactory();

    private final ObjectMapper defaultObjectMapper;

    ObjectMapperFactory() {
        defaultObjectMapper = newObjectMapper();
    }

    public static ObjectMapperFactory instance() {
        return INSTANCE;
    }

    public ObjectMapper getDefaultObjectMapper() {
        return defaultObjectMapper;
    }

    public ObjectMapper newObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JodaModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .configure(SerializationFeature.WRITE_DATES_WITH_ZONE_ID, true)
                .configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(DeserializationFeature.READ_ENUMS_USING_TO_STRING, true)
                .configure(DeserializationFeature.FAIL_ON_UNRESOLVED_OBJECT_IDS, false)
                .configure(DeserializationFeature.FAIL_ON_MISSING_EXTERNAL_TYPE_ID_PROPERTY, false)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public ObjectMapper newMongoCompatibleObjectMapper() {
        ObjectMapper mapper = newObjectMapper();
        mapper.disable(AUTO_DETECT_SETTERS);
        mapper.disable(AUTO_DETECT_GETTERS);
        VisibilityChecker<?> visibilityChecker = mapper.getSerializationConfig().getDefaultVisibilityChecker();
        return mapper.setVisibility(visibilityChecker.withFieldVisibility(ANY))
            .registerModule(new MongoModule());
    }
}
