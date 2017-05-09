package org.janelia.jacs2.model.sage;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.model.BaseEntity;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class SlideImage implements BaseEntity {
    private Integer id;
    private String name;
    private String url;
    private String path;
    private String jfsPath;
    private Date captureDate;
    private String createdBy;
    private Date createDate;
    private String dataset;
    private String lineName;
    private String slideCode;
    private String area;
    private String objective;
    private String tile;
    private Map<String, String> properties = new LinkedHashMap<>();

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

    public String getDataset() {
        return dataset;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public String getLineName() {
        return lineName;
    }

    public void setLineName(String lineName) {
        this.lineName = lineName;
    }

    public String getSlideCode() {
        return slideCode;
    }

    public void setSlideCode(String slideCode) {
        this.slideCode = slideCode;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getObjective() {
        return objective;
    }

    public void setObjective(String objective) {
        this.objective = objective;
    }

    public String getTile() {
        return tile;
    }

    public void setTile(String tile) {
        this.tile = tile;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void addProperty(String name, String value) {
        if (StringUtils.isNotBlank(value)) this.properties.put(name, value);
    }
}
