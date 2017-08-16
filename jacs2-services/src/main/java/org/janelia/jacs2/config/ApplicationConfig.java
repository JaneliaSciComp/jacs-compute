package org.janelia.jacs2.config;

import java.util.Properties;

public class ApplicationConfig extends Properties {
    public String getStringPropertyValue(String name) {
        return getProperty(name);
    }

    public String getStringPropertyValue(String name, String defaultValue) {
        return getProperty(name, defaultValue);
    }

    public Boolean getBooleanPropertyValue(String name) {
        String stringValue = getStringPropertyValue(name);
        return stringValue == null ? false : Boolean.valueOf(stringValue);
    }

    public Integer getIntegerPropertyValue(String name) {
        String stringValue = getStringPropertyValue(name);
        return stringValue == null ? null : Integer.valueOf(stringValue);
    }

    public Integer getIntegerPropertyValue(String name, Integer defaultValue) {
        String stringValue = getStringPropertyValue(name);
        return stringValue == null ? defaultValue : Integer.valueOf(stringValue);
    }

}
