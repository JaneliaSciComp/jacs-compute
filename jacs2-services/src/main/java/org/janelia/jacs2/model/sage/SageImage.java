package org.janelia.jacs2.model.sage;

import org.janelia.jacs2.model.BaseEntity;

import java.util.Date;

public class SageImage implements BaseEntity {
    private Integer id;
    private String name;
    private String url;
    private String path;
    private String jfsPath;
    private Date captureDate;
    private String createdBy;
    private Date createDate;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getJfsPath() {
        return jfsPath;
    }

    public void setJfsPath(String jfsPath) {
        this.jfsPath = jfsPath;
    }

    public Date getCaptureDate() {
        return captureDate;
    }

    public void setCaptureDate(Date captureDate) {
        this.captureDate = captureDate;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }
}
