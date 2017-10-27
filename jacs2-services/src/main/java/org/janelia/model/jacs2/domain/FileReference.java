package org.janelia.model.jacs2.domain;

import org.janelia.model.jacs2.domain.enums.FileType;

public class FileReference {
    private FileType fileType;
    private String fileName;

    public FileReference() {
    }

    public FileReference(FileType fileType, String fileName) {
        this.fileType = fileType;
        this.fileName = fileName;
    }

    public FileType getFileType() {
        return fileType;
    }

    public void setFileType(FileType fileType) {
        this.fileType = fileType;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}
