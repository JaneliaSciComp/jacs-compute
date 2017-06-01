package org.janelia.it.jacs.model.domain.sample;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.janelia.it.jacs.model.domain.AbstractDomainObject;
import org.janelia.it.jacs.model.domain.FileReference;
import org.janelia.it.jacs.model.domain.Reference;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.it.jacs.model.domain.interfaces.HasRelativeFiles;
import org.janelia.it.jacs.model.domain.support.MongoMapping;

import java.util.List;
import java.util.Map;

/**
 * A neuron fragment segmented from an image by the Neuron Separator. 
 */
@MongoMapping(collectionName="fragment", label="Neuron Fragment")
public class NeuronFragment extends AbstractDomainObject implements HasRelativeFiles {

    private Reference sampleRef;
    private Number separationId;
    private Integer number;
    private String filepath;
    private Integer voxelWeight;
    @JsonIgnore
    private HasFileImpl filesImpl = new HasFileImpl();
    
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
        return filepath;
    }

    public void setFilepath(String filepath) {
        this.filepath = filepath;
    }

    public Integer getVoxelWeight() {
        return voxelWeight;
    }

    public void setVoxelWeight(Integer voxelWeight) {
        this.voxelWeight = voxelWeight;
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
    public Map<String, Object> setFileName(FileType fileType, String fileName) {
        return filesImpl.setFileName(fileType, fileName);
    }

    @Override
    public Map<String, Object> removeFileName(FileType fileType) {
        return filesImpl.removeFileName(fileType);
    }

    @JsonProperty
    public List<FileReference> getDeprecatedFiles() {
        return filesImpl.getDeprecatedFiles();
    }

    public void setDeprecatedFiles(List<FileReference> deprecatedFiles) {
        this.filesImpl.setDeprecatedFiles(deprecatedFiles);
    }
}
