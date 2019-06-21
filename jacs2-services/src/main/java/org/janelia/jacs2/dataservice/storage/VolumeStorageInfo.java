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
    private String storageVirtualPath;
    @JsonProperty
    private String storageServiceURL;

    public String getId() {
        return id;
    }

    void setId(String id) {
        this.id = id;
    }

    public String getBaseStorageRootDir() {
        return baseStorageRootDir;
    }

    void setBaseStorageRootDir(String baseStorageRootDir) {
        this.baseStorageRootDir = baseStorageRootDir;
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
