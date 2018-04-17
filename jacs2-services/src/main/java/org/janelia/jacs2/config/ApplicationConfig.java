package org.janelia.jacs2.config;

import org.apache.commons.lang3.StringUtils;

public class ApplicationConfig extends XProperties {
    public String getStringPropertyValue(String name) {
        return getProperty(name);
    }

    public String getStringPropertyValue(String name, String defaultValue) {
        return getProperty(name, defaultValue);
    }

    public Boolean getBooleanPropertyValue(String name) {
        return getBooleanPropertyValue(name, false);
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

}
