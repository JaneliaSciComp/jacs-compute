package org.janelia.jacs2.asyncservice.common;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class ResourceHelper {

    public static String getAuthToken(Map<String, String> serviceResources) {
        return serviceResources.get("authToken");
    }

    public static void setAuthToken(Map<String, String> serviceResources, String authToken) {
        if (StringUtils.isNotBlank(authToken)) {
            serviceResources.put("authToken", authToken);
        }
    }
}
