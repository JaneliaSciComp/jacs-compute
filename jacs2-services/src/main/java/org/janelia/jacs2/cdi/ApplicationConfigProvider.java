package org.janelia.jacs2.cdi;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.config.ApplicationConfigImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class ApplicationConfigProvider {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationConfigProvider.class);

    private static final String DEFAULT_APPLICATION_CONFIG_RESOURCES = "/jacs.properties";

    private static final Map<String, String> APPLICATION_ARGS = new HashMap<>();

    public static Map<String, String> applicationArgs() {
        return APPLICATION_ARGS;
    }

    private ApplicationConfig applicationConfig = new ApplicationConfigImpl();

    public ApplicationConfigProvider fromDefaultResources() {
        return fromProperties(System.getProperties())
                .fromMap(System.getenv().entrySet().stream().collect(Collectors.toMap(entry -> "env." + entry.getKey(), Map.Entry::getValue)))
                .fromResource(DEFAULT_APPLICATION_CONFIG_RESOURCES);
    }

    public ApplicationConfigProvider fromResource(String resourceName) {
        if (StringUtils.isBlank(resourceName)) {
            return this;
        }
        try (InputStream configStream = this.getClass().getResourceAsStream(resourceName)) {
            LOG.info("Reading application config from resource {}", resourceName);
            return fromInputStream(configStream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public ApplicationConfigProvider fromEnvVar(String envVarName) {
        if (StringUtils.isBlank(envVarName)) {
            return this;
        }
        String envVarValue = System.getenv(envVarName);
        if (StringUtils.isBlank(envVarValue)) {
            return this;
        }
        LOG.info("Reading application config from environment {} -> {}", envVarName, envVarValue);
        return fromFile(envVarValue);
    }

    public ApplicationConfigProvider fromFile(String fileName) {
        if (StringUtils.isBlank(fileName)) {
            return this;
        }
        File file = new File(fileName);
        if (file.exists() && file.isFile()) {
            try (InputStream fileInputStream = new FileInputStream(file)) {
                LOG.info("Reading application config from file {}", file);
                return fromInputStream(fileInputStream);
            } catch (IOException e) {
                LOG.error("Error reading configuration file {}", fileName, e);
                throw new UncheckedIOException(e);
            }
        } else {
            LOG.warn("Configuration file {} not found", fileName);
        }
        return this;
    }

    public ApplicationConfigProvider fromMap(Map<String, String> map) {
        applicationConfig.putAll(map);
        return this;
    }

    public ApplicationConfigProvider fromProperties(Properties properties) {
        properties.stringPropertyNames().forEach(k -> applicationConfig.put(k, properties.getProperty(k)));
        return this;
    }

    private ApplicationConfigProvider fromInputStream(InputStream stream) {
        try {
            applicationConfig.load(stream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return this;
    }

    private void injectEnvProps() {
        final String envPrefix = "env.jacs_";
        applicationConfig.asMap().entrySet().stream()
                .filter(entry -> entry.getKey().toLowerCase().startsWith(envPrefix))
                .forEach(entry -> {
                    String newKey = entry.getKey().substring(envPrefix.length()).replaceAll("_", ".");
                    LOG.debug("Overriding env entry {} with {} -> {}", entry, newKey, entry.getValue());
                    applicationConfig.put(newKey, entry.getValue());
                });
    }

    public ApplicationConfig build() {
        injectEnvProps();
        return applicationConfig;
    }

}
