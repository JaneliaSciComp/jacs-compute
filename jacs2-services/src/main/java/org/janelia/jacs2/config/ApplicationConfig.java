package org.janelia.jacs2.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public interface ApplicationConfig {
    String getStringPropertyValue(String name);
    String getStringPropertyValue(String name, String defaultValue);
    Boolean getBooleanPropertyValue(String name);
    Boolean getBooleanPropertyValue(String name, boolean defaultValue);
    Integer getIntegerPropertyValue(String name);
    Integer getIntegerPropertyValue(String name, Integer defaultValue);
    void load(InputStream stream) throws IOException;
    void put(String key, String value);
    void putAll(Map<String, String> properties);
    Map<String, String> asMap();
}
