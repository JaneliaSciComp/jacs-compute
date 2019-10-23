package org.janelia.jacs2.asyncservice.dataimport;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacs2.dataservice.storage.StorageEntryInfo;

import java.nio.file.Path;
import java.nio.file.Paths;

class StorageContentInfo {
    private StorageEntryInfo remoteInfo;
    private String localBasePath;
    private String localRelativePath;

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

    @JsonIgnore
    String getLocalFullPath() {
        return buildFullPath(localBasePath, localRelativePath);
    }

    @JsonIgnore
    String getRemoteFullPath() {
        return buildFullPath(remoteInfo.getStorageRootLocation(), remoteInfo.getEntryRelativePath());
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
