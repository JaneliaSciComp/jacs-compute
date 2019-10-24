package org.janelia.jacs2.asyncservice.dataimport;

import java.nio.file.Paths;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacs2.dataservice.storage.StorageEntryInfo;

class StorageContentInfo {
    @JsonProperty
    private StorageEntryInfo remoteInfo;
    @JsonProperty
    private String localBasePath;
    @JsonProperty
    private String localRelativePath;
    private boolean locallyReachable;

    StorageEntryInfo getRemoteInfo() {
        return remoteInfo;
    }

    void setRemoteInfo(StorageEntryInfo remoteInfo) {
        this.remoteInfo = remoteInfo;
    }

    String getLocalBasePath() {
        return localBasePath;
    }

    void setLocalBasePath(String localBasePath) {
        this.localBasePath = localBasePath;
    }

    String getLocalRelativePath() {
        return localRelativePath;
    }

    void setLocalRelativePath(String localRelativePath) {
        this.localRelativePath = localRelativePath;
    }

    String getLocalFullPath() {
        return buildFullPath(localBasePath, localRelativePath);
    }

    String getRemoteFullPath() {
        return buildFullPath(remoteInfo.getStorageRootLocation(), remoteInfo.getEntryRelativePath());
    }

    boolean isLocallyReachable() {
        return locallyReachable;
    }

    void setLocallyReachable(boolean locallyReachable) {
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
