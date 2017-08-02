package org.janelia.it.jacs.model.domain.sample;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.janelia.it.jacs.model.domain.FileReference;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.it.jacs.model.domain.interfaces.HasFiles;
import org.janelia.jacs2.model.EntityFieldValueHandler;
import org.janelia.jacs2.model.SetFieldValueHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class HasFileImpl implements HasFiles {
    // deprecated files is to keep track of the files that at one point were associated with the domain object
    private List<FileReference> deprecatedFiles = new ArrayList<>();
    private Map<FileType, String> files = new HashMap<>();

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
            String existingFile = getFileName(fileType);
            if (StringUtils.isNotBlank(existingFile) && !StringUtils.equals(existingFile, fileName)) {
                deprecatedFiles.add(new FileReference(fileType, existingFile));
                updates.put("deprecatedFiles", new SetFieldValueHandler<>(deprecatedFiles));
            }
            files.put(fileType, fileName);
            updates.put("files." + fileType.name(), new SetFieldValueHandler<>(fileName));
            return updates;
        }
    }

    @Override
    public Map<String, EntityFieldValueHandler<?>> removeFileName(FileType fileType) {
        Map<String, EntityFieldValueHandler<?>> updates = new LinkedHashMap<>();
        String existingFile = getFileName(fileType);
        if (StringUtils.isNotBlank(existingFile)) {
            deprecatedFiles.add(new FileReference(fileType, existingFile));
            updates.put("deprecatedFiles", new SetFieldValueHandler<>(deprecatedFiles));
        }
        files.remove(fileType);
        updates.put("files." + fileType.name(), null);
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
