package org.janelia.it.jacs.model.domain.sample;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.janelia.it.jacs.model.domain.AbstractDomainObject;
import org.janelia.it.jacs.model.domain.FileReference;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.it.jacs.model.domain.interfaces.HasRelativeFiles;
import org.janelia.it.jacs.model.domain.support.MongoMapping;
import org.janelia.it.jacs.model.domain.support.SAGEAttribute;
import org.janelia.it.jacs.model.domain.support.SearchAttribute;
import org.janelia.jacs2.model.EntityFieldValueHandler;
import org.janelia.jacs2.model.SetFieldValueHandler;

import java.util.List;
import java.util.Map;

/**
 * Image holds a raw acquired image.
 */
@MongoMapping(collectionName="image", label="Image")
public class Image extends AbstractDomainObject implements HasRelativeFiles {
    @SAGEAttribute(cvName = "jacs_calculated", termName = "image_size")
    @SearchAttribute(key = "image_size_s", label = "Image Size")
    private String imageSize;

    @SAGEAttribute(cvName = "jacs_calculated", termName = "optical_resolution")
    @SearchAttribute(key = "optical_res_s", label = "Optical Resolution")
    private String opticalResolution;

    @SearchAttribute(key = "objective_txt", label = "Objective", facet = "objective_s")
    private String objective;

    @SAGEAttribute(cvName = "light_imagery", termName = "channels")
    @SearchAttribute(key = "num_channels_i", label = "Num Channels", facet = "num_channels_i")
    private Integer numChannels;

    // files are in fact alternate representations of this image instance
    @JsonIgnore
    private HasRelativeFilesImpl relativeFilesImpl = new HasRelativeFilesImpl();

    @Override
    public String getFilepath() {
        return relativeFilesImpl.getFilepath();
    }

    public void setFilepath(String filepath) {
        relativeFilesImpl.setFilepath(filepath);
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

    @JsonProperty
    public List<FileReference> getDeprecatedFiles() {
        return relativeFilesImpl.getDeprecatedFiles();
    }

    public void setDeprecatedFiles(List<FileReference> deprecatedFiles) {
        this.relativeFilesImpl.setDeprecatedFiles(deprecatedFiles);
    }

}
