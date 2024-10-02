package org.janelia.jacs2.dataservice.storage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * This is the equivalent of the jade.JacsStorageVolume type.
 */
public class JadeStorageVolume {
    @JsonProperty
    private String id;
    @JsonProperty
    private String storageType;
    @JsonProperty
    private String storageRootLocation;
    @JsonProperty
    private String storageVirtualPath;
    @JsonProperty
    private String storageServiceURL;

    public String getId() {
        return id;
    }

    void setId(String id) {
        this.id = id;
    }

    public String getStorageType() {
        return storageType;
    }

    void setStorageType(String storageType) {
        this.storageType = storageType;
    }

    public String getStorageRootLocation() {
        return storageRootLocation;
    }

    void setStorageRootLocation(String storageRootLocation) {
        this.storageRootLocation = storageRootLocation;
    }

    public String getStorageServiceURL() {
        return storageServiceURL;
    }

    void setStorageServiceURL(String storageServiceURL) {
        this.storageServiceURL = storageServiceURL;
    }

    public String getStorageVirtualPath() {
        return storageVirtualPath;
    }

    void setStorageVirtualPath(String storageVirtualPath) {
        this.storageVirtualPath = storageVirtualPath;
    }

    @JsonIgnore
    public String getVolumeStorageURI() {
        try {
            return UriBuilder.fromUri(new URI(storageServiceURL)).path("agent_storage/storage_volume").path(id).build().toString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
