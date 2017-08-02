package org.janelia.it.jacs.model.domain.sample;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.it.jacs.model.domain.interfaces.HasRelativeFiles;
import org.janelia.jacs2.model.EntityFieldValueHandler;

import java.util.Map;

/**
 * A group of files with a common parent path.
 */
public class FileGroup implements HasRelativeFiles {

    private String key;
    @JsonIgnore
    private HasRelativeFilesImpl relativeFilesImpl = new HasRelativeFilesImpl();

    public FileGroup() {
    }

    public FileGroup(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public String getFilepath() {
        return relativeFilesImpl.getFilepath();
    }

    public void setFilepath(String filepath) {
        relativeFilesImpl.setFilepath(filepath);
    }

    public Map<FileType, String> getFiles() {
        return relativeFilesImpl.getFiles();
    }

    @Override
    public String getFileName(FileType fileType) {
        return relativeFilesImpl.getFileName(fileType);
    }

    @Override
    public Map<String, EntityFieldValueHandler<?>> setFileName(FileType fileType, String fileName) {
        return relativeFilesImpl.setFileName(fileType, fileName);
    }

    @Override
    public Map<String, EntityFieldValueHandler<?>> removeFileName(FileType fileType) {
        return relativeFilesImpl.removeFileName(fileType);
    }
}
