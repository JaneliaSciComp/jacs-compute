package org.janelia.jacs2.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Base64;
import java.util.List;

// TODO: merge this functionality into the JWTProvider class
public class JwtDecoder {
    private static final Logger LOG = LoggerFactory.getLogger(JwtDecoder.class);

    private final ObjectMapper mapper;

    @Inject
    JwtDecoder(ObjectMapperFactory mapperFactory) {
        this.mapper = mapperFactory.newObjectMapper();
    }

    JWT decode(String token) {
        List<String> tokenComponents = Splitter.on('.')
                .omitEmptyStrings()
                .splitToList(token);
        if (tokenComponents.size() < 3) {
            LOG.error("Token '{}' does not have enough components: {}", token, tokenComponents);
            return new JWT();
        }
        return tokenComponents
                .stream()
                .skip(1)
                .limit(1)
                .map(s -> s + StringUtils.repeat('=', (4 - s.length() % 4) % 4))
                .map(s -> new String(Base64.getDecoder().decode(s)))
                .findFirst()
                .map(s -> {
                    try {
                        return mapper.readValue(s, JWT.class);
                    } catch (IOException e) {
                        LOG.error("Error parsing JWT token component {}", s, e);
                        return new JWT();
                    }
                })
                .orElseGet(() -> new JWT());
    }

}
