package org.janelia.workstation.jfs.mongo;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.bson.types.ObjectId;

import java.util.Map;

@ApiModel(value = "JOSObject", description="An object managed by the Janelia Object Store")
public class JOSObject {

    private ObjectId _id;    
    @ApiModelProperty(value = "Parent Path")
    private String parentPath;
    @ApiModelProperty(value = "Object Path")
    private String path;
    @ApiModelProperty(value = "Object Name", notes = "The file name portion of the path")
    private String name;
    @ApiModelProperty(value = "File Type", notes = "The extension portion of the path")
    private String fileType;
    @ApiModelProperty(value = "Username of the owner")
    private String owner;
    @ApiModelProperty(value = "Number of bytes in the object stream")
    private Long numBytes;
    @ApiModelProperty(value = "Is the object bzip2 compressed inside the object store?")
    private boolean bzipped = false;
    @ApiModelProperty(value = "Has the object been marked for deletion?")
    private boolean deleted = false;
    @ApiModelProperty(value = "The MD5 checksum for the object")
    private String checksum;
    @ApiModelProperty(value = "Identifies how the metadata information is transformed into the object store key")
    private String pathType;
    @ApiModelProperty(value = "Area to store additional useful metadata per store")
    private Map<String,String> additionalInfo;
    
    public ObjectId getId() {
        return _id;
    }
    public void setId(ObjectId id) {
        this._id = id;
    }
    public String getParentPath() {
        return parentPath;
    }
    public void setParentPath(String parentPath) {
        this.parentPath = parentPath;
    }
    public String getPath() {
        return path;
    }
    public void setPath(String path) {
        this.path = path;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getFileType() {
        return fileType;
    }
    public void setFileType(String fileType) {
        this.fileType = fileType;
    }
    public String getOwner() {
        return owner;
    }
    public void setOwner(String owner) {
        this.owner = owner;
    }
    public Long getNumBytes() {
        return numBytes;
    }
    public void setNumBytes(Long numBytes) {
        this.numBytes = numBytes;
    }
    public boolean isBzipped() {
        return bzipped;
    }
    public void setBzipped(boolean bzipped) {
        this.bzipped = bzipped;
    }
    public boolean isDeleted() { return deleted; }
    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }
    public String getChecksum() { return checksum; }
    public void setChecksum(String checksum) { this.checksum = checksum; }
    public Map<String, String> getAdditionalInfo() { return additionalInfo; }
    public void setAdditionalInfo(Map<String, String> additionalInfo) { this.additionalInfo = additionalInfo; }
    public String getPathType() {return pathType;}
    public void setPathType(String pathType) {this.pathType = pathType;}
}
