package org.janelia.it.jacs.model.domain.sample;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.janelia.it.jacs.model.domain.FileReference;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.it.jacs.model.domain.interfaces.HasFiles;
import org.janelia.it.jacs.model.domain.interfaces.HasRelativeFiles;

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
    public Map<String, Object> setFileName(FileType fileType, String fileName) {
        if (StringUtils.isBlank(fileName)) {
            return removeFileName(fileType);
        } else {
            Map<String, Object> updates = new LinkedHashMap<>();
            Path existingFile = getFullFilePath(fileType);
            Path newFileTypePath = Paths.get(fileName);
            if (existingFile != null && existingFile.equals(newFileTypePath)) {
                // there's no change here so simply return
                return updates;
            } else if (existingFile != null) {
                deprecatedFiles.add(new FileReference(fileType, existingFile.toString()));
                updates.put("deprecatedFiles", deprecatedFiles);
            }
            updates.putAll(updateFileName(fileType, newFileTypePath));
            return updates;
        }
    }

    private Map<String, Object> updateFileName(FileType fileType, Path fileTypePath) {
        Map<String, Object> updates = new LinkedHashMap<>();
        if (!hasFilepath()) {
            files.put(fileType, fileTypePath.toString());
            updates.put("files." + fileType.name(), fileTypePath.toString());
        } else {
            Path currentFilePath = Paths.get(filepath);
            if (!fileTypePath.startsWith(currentFilePath) || fileTypePath.equals(currentFilePath)) {
                // if the new file path does not start with filepath or the new file's path is identical to the current file path then put it as is
                files.put(fileType, fileTypePath.toString());
                updates.put("files." + fileType.name(), fileTypePath.toString());
            } else {
                Path relativeFileTypePath = currentFilePath.relativize(fileTypePath);
                files.put(fileType, relativeFileTypePath.toString());
                updates.put("files." + fileType.name(), relativeFileTypePath.toString());
            }
        }
        return updates;
    }

    @Override
    public Map<String, Object> removeFileName(FileType fileType) {
        Map<String, Object> updates = new LinkedHashMap<>();
        Path existingFile = getFullFilePath(fileType);
        if (existingFile != null) {
            files.remove(fileType);
            deprecatedFiles.add(new FileReference(fileType, existingFile.toString()));
            updates.put("deprecatedFiles", deprecatedFiles);
            updates.put("files." + fileType.name(), null);
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
