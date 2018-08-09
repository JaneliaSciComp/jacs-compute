package org.janelia.jacs2.dataservice.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Optional;

/**
 * This is the equivalent of the jade.DataNodeInfo type.
 */
public class StorageEntryInfo {
    private final String storageId;
    private final String storageURL;
    private final String entryURL;
    private final String storageRootLocation;
    private final StoragePathURI storageRootPathURI;
    private final String entryRelativePath;
    private final boolean collectionFlag;

    @JsonCreator
    public StorageEntryInfo(@JsonProperty("storageId") String storageId,
                            @JsonProperty("storageURL") String storageURL,
                            @JsonProperty("entryURL") String entryURL,
                            @JsonProperty("storageRootLocation") String storageRootLocation,
                            @JsonProperty("storageRootPathURI") StoragePathURI storageRootPathURI,
                            @JsonProperty("entryRelativePath") String entryRelativePath,
                            @JsonProperty("collectionFlag") boolean collectionFlag) {
        this.storageId = storageId;
        this.storageURL = storageURL;
        this.entryURL = entryURL;
        this.storageRootLocation = storageRootLocation;
        this.storageRootPathURI = storageRootPathURI;
        this.entryRelativePath = entryRelativePath;
        this.collectionFlag = collectionFlag;
    }

    public String getStorageId() {
        return storageId;
    }

    public String getStorageURL() {
        return storageURL;
    }

    public String getEntryURL() {
        return entryURL;
    }

    public String getStorageRootLocation() {
        return storageRootLocation;
    }

    public StoragePathURI getStorageRootPathURI() {
        return storageRootPathURI;
    }

    public String getEntryRelativePath() {
        return entryRelativePath;
    }

    public Optional<StoragePathURI> getEntryPathURI() {
        return storageRootPathURI.resolve(entryRelativePath);
    }

    boolean isCollectionFlag() {
        return collectionFlag;
    }

    @JsonIgnore
    public boolean isCollection() {
        return isCollectionFlag();
    }

    @JsonIgnore
    public boolean isNotCollection() {
        return !isCollectionFlag();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("storageId", storageId)
                .append("storageURL", storageURL)
                .append("entryURL", entryURL)
                .append("storageRootLocation", storageRootLocation)
                .append("storageRootPathURI", storageRootPathURI)
                .append("entryRelativePath", entryRelativePath)
                .append("collectionFlag", collectionFlag)
                .toString();
    }
}
