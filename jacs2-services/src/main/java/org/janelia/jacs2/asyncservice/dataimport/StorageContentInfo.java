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
    public Path getLocalFullPath() {
        if (StringUtils.isBlank(localBasePath) && StringUtils.isBlank(localRelativePath)) {
            return null;
        } else if (StringUtils.isBlank(localBasePath)) {
            return Paths.get(localRelativePath);
        } else if (StringUtils.isBlank(localRelativePath)) {
            return Paths.get(localBasePath);
        } else {
            return Paths.get(localBasePath, localRelativePath);
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
