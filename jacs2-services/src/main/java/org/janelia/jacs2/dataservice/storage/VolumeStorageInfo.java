package org.janelia.jacs2.dataservice.storage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * This is the equivalent of the jade.JacsStorageVolume type.
 */
public class VolumeStorageInfo {
    @JsonProperty
    private String id;
    @JsonProperty
    private String baseStorageRootDir;
    @JsonProperty
    private String storageServiceURL;

    void setId(String id) {
        this.id = id;
    }

    void setBaseStorageRootDir(String baseStorageRootDir) {
        this.baseStorageRootDir = baseStorageRootDir;
    }

    void setStorageServiceURL(String storageServiceURL) {
        this.storageServiceURL = storageServiceURL;
    }

    public String getBaseStorageRootDir() {
        return baseStorageRootDir;
    }

    @JsonIgnore
    public String getStorageURL() {
        try {
            return UriBuilder.fromUri(new URI(storageServiceURL)).path("agent_storage/storage_volume").path(id).build().toString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
