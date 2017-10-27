package org.janelia.model.jacs2.domain.sample;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.janelia.model.jacs2.domain.FileReference;
import org.janelia.model.jacs2.domain.enums.FileType;
import org.janelia.model.jacs2.domain.interfaces.HasRelativeFiles;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.jacs2.SetFieldValueHandler;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class HasRelativeFilesImpl implements HasRelativeFiles {
    private String filepath;
    // deprecated files is to keep track of the files that at one point were associated with the domain object
    private List<FileReference> deprecatedFiles = new ArrayList<>();
    private Map<FileType, String> files = new HashMap<>();

    @Override
    public String getFilepath() {
        return filepath;
    }

    public void setFilepath(String filepath) {
        this.filepath = filepath;
    }

    @Override
    public Map<FileType, String> getFiles() {
        return files;
    }

    @Override
    public String getFileName(FileType fileType) {
        return files.get(fileType);
    }

    @Override
    public Map<String, EntityFieldValueHandler<?>> setFileName(FileType fileType, String fileName) {
        if (StringUtils.isBlank(fileName)) {
            return removeFileName(fileType);
        } else {
            Map<String, EntityFieldValueHandler<?>> updates = new LinkedHashMap<>();
            Path existingFile = getFullFilePath(fileType);
            Path newFileTypePath = Paths.get(fileName);
            if (existingFile != null && existingFile.equals(newFileTypePath)) {
                // there's no change here so simply return
                return updates;
            } else if (existingFile != null) {
                deprecatedFiles.add(new FileReference(fileType, existingFile.toString()));
                updates.put("deprecatedFiles", new SetFieldValueHandler<>(deprecatedFiles));
            }
            updates.putAll(updateFileName(fileType, newFileTypePath));
            return updates;
        }
    }

    private Map<String, EntityFieldValueHandler<?>> updateFileName(FileType fileType, Path fileTypePath) {
        Map<String, EntityFieldValueHandler<?>> updates = new LinkedHashMap<>();
        if (!hasFilepath()) {
            files.put(fileType, fileTypePath.toString());
            updates.put("files." + fileType.name(), new SetFieldValueHandler<>(fileTypePath.toString()));
        } else {
            Path currentFilePath = Paths.get(filepath);
            if (!fileTypePath.startsWith(currentFilePath) || fileTypePath.equals(currentFilePath)) {
                // if the new file path does not start with filepath or the new file's path is identical to the current file path then put it as is
                files.put(fileType, fileTypePath.toString());
                updates.put("files." + fileType.name(), new SetFieldValueHandler<>(fileTypePath.toString()));
            } else {
                Path relativeFileTypePath = currentFilePath.relativize(fileTypePath);
                files.put(fileType, relativeFileTypePath.toString());
                updates.put("files." + fileType.name(), new SetFieldValueHandler<>(relativeFileTypePath.toString()));
            }
        }
        return updates;
    }

    @Override
    public Map<String, EntityFieldValueHandler<?>> removeFileName(FileType fileType) {
        Map<String, EntityFieldValueHandler<?>> updates = new LinkedHashMap<>();
        Path existingFile = getFullFilePath(fileType);
        if (existingFile != null) {
            files.remove(fileType);
            deprecatedFiles.add(new FileReference(fileType, existingFile.toString()));
            updates.put("deprecatedFiles", new SetFieldValueHandler<>(deprecatedFiles));
            updates.put("files." + fileType.name(), new SetFieldValueHandler<>(null));
        }
        return updates;
    }

    public List<FileReference> getDeprecatedFiles() {
        return deprecatedFiles;
    }

    public void setDeprecatedFiles(List<FileReference> deprecatedFiles) {
        this.deprecatedFiles = deprecatedFiles;
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this, false);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj, false);
    }
}
