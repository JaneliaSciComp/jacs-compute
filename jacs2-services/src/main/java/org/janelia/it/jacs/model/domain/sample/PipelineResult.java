package org.janelia.it.jacs.model.domain.sample;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.janelia.it.jacs.model.domain.FileReference;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.it.jacs.model.domain.interfaces.HasRelativeFiles;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * The result of some processing. May be nested if further processing is done on this result.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
public class PipelineResult implements HasRelativeFiles {

    private Long id;
    private String name;
    private String filepath;
    private Date creationDate;
    private List<PipelineResult> results = new ArrayList<>();
    @JsonIgnore
    private HasFileImpl filesImpl = new HasFileImpl();

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getFilepath() {
        return filepath;
    }

    public void setFilepath(String filepath) {
        this.filepath = filepath;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    public List<PipelineResult> getResults() {
        return results;
    }

    public void setResults(List<PipelineResult> results) {
        this.results = results;
    }

    @Override
    public Map<FileType, String> getFiles() {
        return filesImpl.getFiles();
    }

    @Override
    public String getFileName(FileType fileType) {
        return filesImpl.getFileName(fileType);
    }

    @Override
    public void setFileName(FileType fileType, String fileName) {
        filesImpl.setFileName(fileType, fileName);
    }

    @Override
    public void removeFileName(FileType fileType) {
        filesImpl.removeFileName(fileType);
    }

    @JsonProperty
    public List<FileReference> getDeprecatedFiles() {
        return filesImpl.getDeprecatedFiles();
    }

    public void setDeprecatedFiles(List<FileReference> deprecatedFiles) {
        this.filesImpl.setDeprecatedFiles(deprecatedFiles);
    }
}
