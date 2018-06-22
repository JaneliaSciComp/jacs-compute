package org.janelia.jacs2.asyncservice.dataimport;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.model.domain.enums.FileType;

import java.nio.file.Path;

class StorageContentInfo {
    private Number dataNodeId;
    private StorageService.StorageEntryInfo remoteInfo;
    private Path localBasePath;
    private Path localRelativePath;
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

    public Path getLocalBasePath() {
        return localBasePath;
    }

    public void setLocalBasePath(Path localBasePath) {
        this.localBasePath = localBasePath;
    }

    public Path getLocalRelativePath() {
        return localRelativePath;
    }

    public void setLocalRelativePath(Path localRelativePath) {
        this.localRelativePath = localRelativePath;
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
