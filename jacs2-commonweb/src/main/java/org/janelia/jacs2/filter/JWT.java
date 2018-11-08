package org.janelia.jacs2.filter;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;

import java.util.Date;

class JWT {
    @JsonProperty("user_name")
    String userName;
    @JsonProperty("exp")
    Long expInSeconds;

    boolean isValid() {
        return StringUtils.isNotBlank(userName) && expInSeconds != null && expInSeconds * 1000 > System.currentTimeMillis();
    }
}
