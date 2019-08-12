package org.janelia.jacs2.config;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.janelia.configutils.ConfigValueResolver;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ApplicationConfigImpl implements ApplicationConfig {
    private final Map<String, String> configProperties = new HashMap<>();
    private final ConfigValueResolver configValueResolver = new ConfigValueResolver();

    @Override
    public String getStringPropertyValue(String name) {
        String value = configProperties.get(name);
        return configValueResolver.resolve(value, configProperties::get);
    }

    public String getStringPropertyValue(String name, String defaultValue) {
        String value = getStringPropertyValue(name);
        return value == null ? defaultValue : value;
    }

    public Boolean getBooleanPropertyValue(String name) {
        String stringValue = getStringPropertyValue(name);
        return StringUtils.isBlank(stringValue) ? Boolean.FALSE : Boolean.valueOf(stringValue);
    }

    public Boolean getBooleanPropertyValue(String name, boolean defaultValue) {
        String stringValue = getStringPropertyValue(name);
        return StringUtils.isBlank(stringValue) ? defaultValue : Boolean.valueOf(stringValue);
    }

    public Integer getIntegerPropertyValue(String name) {
        String stringValue = getStringPropertyValue(name);
        return StringUtils.isBlank(stringValue) ? null : Integer.valueOf(stringValue);
    }

    public Integer getIntegerPropertyValue(String name, Integer defaultValue) {
        String stringValue = getStringPropertyValue(name);
        return StringUtils.isBlank(stringValue) ? defaultValue : Integer.valueOf(stringValue);
    }

    @Override
    public Long getLongPropertyValue(String name) {
        String stringValue = getStringPropertyValue(name);
        return StringUtils.isBlank(stringValue) ? null : Long.valueOf(stringValue);
    }

    @Override
    public Long getLongPropertyValue(String name, Long defaultValue) {
        String stringValue = getStringPropertyValue(name);
        return StringUtils.isBlank(stringValue) ? defaultValue : Long.valueOf(stringValue);
    }

    @Override
    public List<String> getStringListPropertyValue(String name) {
        return getStringListPropertyValue(name, ImmutableList.of());
    }

    @Override
    public List<String> getStringListPropertyValue(String name, List<String> defaultValue) {
        String stringValue = getStringPropertyValue(name);
        if (StringUtils.isBlank(stringValue)) {
            return defaultValue;
        } else {
            return Splitter.on(',').trimResults().splitToList(stringValue);
        }
    }

    @Override
    public void load(InputStream stream) throws IOException {
        Properties toLoad = new Properties();
        toLoad.load(stream);
        putAll(Maps.fromProperties(toLoad));
    }

    @Override
    public void put(String key, String value) {
        configProperties.put(key, value);
    }

    @Override
    public void putAll(Map<String, String> properties) {
        configProperties.putAll(properties);
    }

    @Override
    public Map<String, String> asMap() {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        configProperties.keySet().forEach(k -> {
            String v = getStringPropertyValue(k);
            if (k != null && v != null) builder.put(k, v);
        });
        return builder.build();
    }
}
