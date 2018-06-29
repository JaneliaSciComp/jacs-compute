package org.janelia.jacs2.asyncservice.dataimport;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.model.domain.enums.FileType;

import java.nio.file.Path;
import java.nio.file.Paths;

class StorageContentInfo {
    private Number dataNodeId;
    private StorageService.StorageEntryInfo remoteInfo;
    private String localBasePath;
    private String localRelativePath;
    private FileType fileType;

    public Number getDataNodeId() {
        return dataNodeId;
    }

    public void setDataNodeId(Number dataNodeId) {
        this.dataNodeId = dataNodeId;
    }

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
        return StringUtils.isNotBlank(localBasePath) ? Paths.get(localBasePath, localRelativePath) : Paths.get(localRelativePath);
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
                .append("dataNodeId", dataNodeId)
                .append("remoteInfo", remoteInfo)
                .append("localBasePath", localBasePath)
                .append("localRelativePath", localRelativePath)
                .append("fileType", fileType)
                .toString();
    }
}
