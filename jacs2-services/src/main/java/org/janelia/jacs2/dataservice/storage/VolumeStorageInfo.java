package org.janelia.jacs2.dataservice.storage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * This is the equivalent of the jade.JacsStorageVolume type.
 */
public class VolumeStorageInfo {
    @JsonProperty("_id")
    private String storageId;
    private String name;
    private StoragePathURI storageRootPathURI;
    private String volumeRootDir;
    private String baseStorageRootDir;
    private String storageVirtualPath;
    private String storageServiceURL;
    private String storageHost;
    private List<String> storageTags;

    public String getStorageId() {
        return storageId;
    }

    public void setStorageId(String storageId) {
        this.storageId = storageId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public StoragePathURI getStorageRootPathURI() {
        return storageRootPathURI;
    }

    public void setStorageRootPathURI(StoragePathURI storageRootPathURI) {
        this.storageRootPathURI = storageRootPathURI;
    }

    public String getVolumeRootDir() {
        return volumeRootDir;
    }

    public void setVolumeRootDir(String volumeRootDir) {
        this.volumeRootDir = volumeRootDir;
    }

    public String getBaseStorageRootDir() {
        return baseStorageRootDir;
    }

    public void setBaseStorageRootDir(String baseStorageRootDir) {
        this.baseStorageRootDir = baseStorageRootDir;
    }

    public String getStorageVirtualPath() {
        return storageVirtualPath;
    }

    public void setStorageVirtualPath(String storageVirtualPath) {
        this.storageVirtualPath = storageVirtualPath;
    }

    public String getStorageServiceURL() {
        return storageServiceURL;
    }

    public void setStorageServiceURL(String storageServiceURL) {
        this.storageServiceURL = storageServiceURL;
    }

    public String getStorageHost() {
        return storageHost;
    }

    public void setStorageHost(String storageHost) {
        this.storageHost = storageHost;
    }

    public List<String> getStorageTags() {
        return storageTags;
    }

    public void setStorageTags(List<String> storageTags) {
        this.storageTags = storageTags;
    }

    @JsonIgnore
    public String getStorageURL() {
        try {
            return UriBuilder.fromUri(new URI(storageServiceURL)).path("agent_storage/storage_volume").path(getStorageId()).build().toString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
