package org.janelia.model.jacs2.domain.sample;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.janelia.model.domain.Reference;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.jacs2.domain.AbstractDomainObject;
import org.janelia.model.jacs2.domain.FileReference;
import org.janelia.model.jacs2.domain.enums.FileType;
import org.janelia.model.jacs2.domain.interfaces.HasRelativeFiles;
import org.janelia.model.jacs2.domain.support.MongoMapping;

/**
 * A neuron fragment segmented from an image by the Neuron Separator. 
 */
@MongoMapping(collectionName="fragment", label="Neuron Fragment")
public class NeuronFragment extends AbstractDomainObject implements HasRelativeFiles {

    private Reference sampleRef;
    private Number separationId;
    private Integer number;
    private Integer voxelWeight;
    @JsonIgnore
    private HasRelativeFilesImpl relativeFilesImpl = new HasRelativeFilesImpl();
    
    public Integer getNumber() {
        return number;
    }

    public Reference getSample() {
		return sampleRef;
	}

    public void setSample(Reference sample) {
		this.sampleRef = sample;
	}

    public Number getSeparationId() {
        return separationId;
    }

    public void setSeparationId(Number separationId) {
        this.separationId = separationId;
    }
    
    public void setNumber(Integer number) {
        this.number = number;
    }

    public String getFilepath() {
        return relativeFilesImpl.getFilepath();
    }

    public void setFilepath(String filepath) {
        relativeFilesImpl.setFilepath(filepath);
    }

    public Integer getVoxelWeight() {
        return voxelWeight;
    }

    public void setVoxelWeight(Integer voxelWeight) {
        this.voxelWeight = voxelWeight;
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
