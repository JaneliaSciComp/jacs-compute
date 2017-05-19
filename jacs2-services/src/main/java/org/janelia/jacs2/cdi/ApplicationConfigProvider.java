package org.janelia.jacs2.cdi;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.config.ApplicationConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

public class ApplicationConfigProvider {

    private static final String DEFAULT_APPLICATION_CONFIG_RESOURCES = "/jacs.properties";

    private static final Map<String, String> APPLICATION_ARGS = new HashMap<>();

    public static Map<String, String> applicationArgs() {
        return APPLICATION_ARGS;
    }

    private ApplicationConfig applicationConfig = new ApplicationConfig();

    public ApplicationConfigProvider fromDefaultResource() {
        return fromResource(DEFAULT_APPLICATION_CONFIG_RESOURCES);
    }

    public ApplicationConfigProvider fromResource(String resourceName) {
        if (StringUtils.isBlank(resourceName)) {
            return this;
        }
        try (InputStream configStream = this.getClass().getResourceAsStream(resourceName)) {
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
        return fromFile(envVarValue);
    }

    public ApplicationConfigProvider fromFile(String fileName) {
        if (StringUtils.isBlank(fileName)) {
            return this;
        }
        File file = new File(fileName);
        if (file.exists() && file.isFile()) {
            try (InputStream fileInputStream = new FileInputStream(file)) {
                return fromInputStream(fileInputStream);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return this;
    }

    public ApplicationConfigProvider fromMap(Map<String, String> map) {
        applicationConfig.putAll(map);
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

    public ApplicationConfig build() {
        return applicationConfig;
    }

}
