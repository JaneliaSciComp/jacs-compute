package org.janelia.it.jacs.model.domain.sample;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.janelia.it.jacs.model.domain.AbstractDomainObject;
import org.janelia.it.jacs.model.domain.FileReference;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.it.jacs.model.domain.interfaces.HasRelativeFiles;
import org.janelia.it.jacs.model.domain.support.MongoMapping;
import org.janelia.it.jacs.model.domain.support.SAGEAttribute;

import java.util.List;
import java.util.Map;

/**
 * Image holds a raw acquired image.
 */
@MongoMapping(collectionName="image", label="Image")
public class Image extends AbstractDomainObject implements HasRelativeFiles {
    private String filepath;
    @SAGEAttribute(cvName="jacs_calculated", termName="image_size")
    private String imageSize;
    @SAGEAttribute(cvName="jacs_calculated", termName="optical_resolution")
    private String opticalResolution;
    private String objective;
    @SAGEAttribute(cvName="light_imagery", termName="channels")
    private Integer numChannels;
    // files are in fact alternate representations of this image instance
    @JsonIgnore
    private HasFileImpl filesImpl = new HasFileImpl();

    @Override
    public String getFilepath() {
        return filepath;
    }

    public void setFilepath(String filepath) {
        this.filepath = filepath;
    }

    public String getImageSize() {
        return imageSize;
    }

    public void setImageSize(String imageSize) {
        this.imageSize = imageSize;
    }

    public String getOpticalResolution() {
        return opticalResolution;
    }

    public void setOpticalResolution(String opticalResolution) {
        this.opticalResolution = opticalResolution;
    }

    public String getObjective() {
        return objective;
    }

    public void setObjective(String objective) {
        this.objective = objective;
    }

    public Integer getNumChannels() {
        return numChannels;
    }

    public void setNumChannels(Integer numChannels) {
        this.numChannels = numChannels;
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
