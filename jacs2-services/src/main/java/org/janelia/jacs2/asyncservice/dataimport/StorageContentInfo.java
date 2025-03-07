package org.janelia.jacs2.asyncservice.dataimport;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.clients.api.StorageEntryInfo;

import java.nio.file.Paths;

public class StorageContentInfo {
    @JsonProperty
    private StorageEntryInfo remoteInfo;
    @JsonProperty
    private String localBasePath;
    @JsonProperty
    private String localRelativePath;
    @JsonProperty
    private Long size;
    private boolean locallyReachable;

    public StorageEntryInfo getRemoteInfo() {
        return remoteInfo;
    }

    public void setRemoteInfo(StorageEntryInfo remoteInfo) {
        this.remoteInfo = remoteInfo;
    }

    public String getLocalBasePath() {
        return localBasePath;
    }

    public void setLocalBasePath(String localBasePath) {
        this.localBasePath = localBasePath;
    }

    public String getLocalRelativePath() {
        return localRelativePath;
    }

    public void setLocalRelativePath(String localRelativePath) {
        this.localRelativePath = localRelativePath;
    }

    public String getLocalFullPath() {
        return buildFullPath(localBasePath, localRelativePath);
    }

    public String getRemoteFullPath() {
        return buildFullPath(remoteInfo.getStorageRootLocation(), remoteInfo.getEntryRelativePath());
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }

    public boolean isLocallyReachable() {
        return locallyReachable;
    }

    public void setLocallyReachable(boolean locallyReachable) {
        this.locallyReachable = locallyReachable;
    }

    private String buildFullPath(String basePath, String relativePath) {
        if (StringUtils.isBlank(basePath) && StringUtils.isBlank(relativePath)) {
            return null;
        } else if (StringUtils.isBlank(basePath)) {
            return Paths.get(relativePath).toString();
        } else if (StringUtils.isBlank(relativePath)) {
            return Paths.get(basePath).toString();
        } else {
            return Paths.get(basePath, relativePath).toString();
        }

    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("remoteInfo", remoteInfo)
                .append("localBasePath", localBasePath)
                .append("localRelativePath", localRelativePath)
                .toString();
    }
}
