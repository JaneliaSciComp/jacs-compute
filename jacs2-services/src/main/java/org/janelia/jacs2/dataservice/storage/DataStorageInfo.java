package org.janelia.jacs2.dataservice.storage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * This is the equivalent of the jade.DataStorageInfo type.
 */
public class DataStorageInfo {
    @JsonProperty("id")
    private String storageId;
    private String name;
    private String ownerKey;
    private StoragePathURI storageRootPathURI;
    private String storageRootDir;
    private String dataVirtualPath;
    private String dataStorageURI;
    private String path;
    private String storageHost;
    private List<String> storageTags;
    private String connectionURL;
    private String storageFormat;

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

    public String getOwnerKey() {
        return ownerKey;
    }

    public void setOwnerKey(String ownerKey) {
        this.ownerKey = ownerKey;
    }

    public StoragePathURI getStorageRootPathURI() {
        return storageRootPathURI;
    }

    public void setStorageRootPathURI(StoragePathURI storageRootPathURI) {
        this.storageRootPathURI = storageRootPathURI;
    }

    public String getStorageRootDir() {
        return storageRootDir;
    }

    public void setStorageRootDir(String storageRootDir) {
        this.storageRootDir = storageRootDir;
    }

    public String getDataVirtualPath() {
        return dataVirtualPath;
    }

    public void setDataVirtualPath(String dataVirtualPath) {
        this.dataVirtualPath = dataVirtualPath;
    }

    public String getDataStorageURI() {
        return dataStorageURI;
    }

    public void setDataStorageURI(String dataStorageURI) {
        this.dataStorageURI = dataStorageURI;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
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

    public String getConnectionURL() {
        return connectionURL;
    }

    public void setConnectionURL(String connectionURL) {
        this.connectionURL = connectionURL;
    }

    public String getStorageFormat() {
        return storageFormat;
    }

    public void setStorageFormat(String storageFormat) {
        this.storageFormat = storageFormat;
    }

    @JsonIgnore
    public String getStorageURL() {
        try {
            return UriBuilder.fromUri(new URI(getConnectionURL())).path("agent_storage").path(getStorageId()).build().toString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
