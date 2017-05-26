package org.janelia.jacs2.dao.mongo.utils;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.janelia.jacs2.model.jacsservice.ProcessingLocation;

import java.util.HashMap;
import java.util.LinkedHashMap;

public class RegistryHelper {

    public static CodecRegistry createCodecRegistry(ObjectMapperFactory objectMapperFactory) {
        return CodecRegistries.fromRegistries(
                MongoClient.getDefaultCodecRegistry(),
                CodecRegistries.fromCodecs(
                        new BigIntegerCodec(),
                        new EnumCodec<>(JacsServiceState.class),
                        new EnumCodec<>(ProcessingLocation.class),
                        new EnumCodec<>(FileType.class),
                        new MapOfEnumCodec<>(FileType.class, HashMap.class),
                        new MapOfEnumCodec<>(FileType.class, LinkedHashMap.class)
                ),
                CodecRegistries.fromProviders(new DomainCodecProvider(objectMapperFactory))
        );
    }

}
