package org.janelia.jacs2.asyncservice.dataimport;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.model.domain.enums.FileType;

import java.nio.file.Path;
import java.nio.file.Paths;

class StorageContentInfo {
    private StorageService.StorageEntryInfo remoteInfo;
    private String localBasePath;
    private String localRelativePath;
    private FileType fileType;

    public StorageService.StorageEntryInfo getRemoteInfo() {
        return remoteInfo;
    }

    public void setRemoteInfo(StorageService.StorageEntryInfo remoteInfo) {
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
        } else {
            return Paths.get(localBasePath, localRelativePath);
        }
    }

    public FileType getFileType() {
        return fileType;
    }

    public void setFileType(FileType fileType) {
        this.fileType = fileType;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("remoteInfo", remoteInfo)
                .append("localBasePath", localBasePath)
                .append("localRelativePath", localRelativePath)
                .append("fileType", fileType)
                .toString();
    }
}
