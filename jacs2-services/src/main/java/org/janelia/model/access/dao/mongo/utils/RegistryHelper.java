package org.janelia.model.access.dao.mongo.utils;

import com.mongodb.MongoClient;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.model.jacs2.domain.enums.FileType;
import org.janelia.model.service.JacsServiceState;
import org.janelia.model.service.ProcessingLocation;

public class RegistryHelper {

    public static CodecRegistry createCodecRegistry(ObjectMapperFactory objectMapperFactory) {
        return CodecRegistries.fromRegistries(
                MongoClient.getDefaultCodecRegistry(),
                CodecRegistries.fromCodecs(
                        new ReferenceCodec(),
                        new BigIntegerCodec(),
                        new EnumCodec<>(JacsServiceState.class),
                        new EnumCodec<>(ProcessingLocation.class),
                        new EnumCodec<>(FileType.class)
                ),
                CodecRegistries.fromProviders(new JacksonCodecProvider(objectMapperFactory))
        );
    }

}
