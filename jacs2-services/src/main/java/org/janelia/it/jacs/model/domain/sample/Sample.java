package org.janelia.it.jacs.model.domain.sample;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.it.jacs.model.domain.AbstractDomainObject;
import org.janelia.it.jacs.model.domain.IndexedReference;
import org.janelia.it.jacs.model.domain.support.MongoMapping;
import org.janelia.it.jacs.model.domain.support.SAGEAttribute;
import org.janelia.it.jacs.model.domain.support.SearchAttribute;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * All the processing results of a particular specimen. Uniqueness of a Sample is determined by a combination
 * of data set and slide code. A single sample may include many LSMs. For example, it may include images taken
 * at multiple objectives (e.g. 20x/63x), of different anatomical areas (e.g. Brain/VNC), and of different
 * tile regions which are stitched together.
 */
@MongoMapping(collectionName="sample", label="Sample")
public class Sample extends AbstractDomainObject {
    @SAGEAttribute(cvName = "light_imagery", termName = "age")
    @SearchAttribute(key = "age_txt", label = "Age", facet = "age_s")
    private String age;

    @SAGEAttribute(cvName = "light_imagery", termName = "data_set")
    @SearchAttribute(key = "data_set_txt", label = "Data Set", facet = "data_set_s")
    private String dataSet;

    @SAGEAttribute(cvName = "fly", termName = "effector")
    @SearchAttribute(key = "effector_txt", label = "Effector")
    private String effector;

    @SAGEAttribute(cvName = "line", termName = "flycore_alias")
    @SearchAttribute(key = "fcalias_s", label = "Fly Core Alias")
    private String flycoreAlias;

    @SAGEAttribute(cvName = "light_imagery", termName = "gender")
    @SearchAttribute(key = "gender_txt", label = "Gender", facet = "gender_s")
    private String gender;

    @SAGEAttribute(cvName = "light_imagery", termName = "mounting_protocol")
    @SearchAttribute(key = "mount_protocol_txt", label = "Mounting Protocol")
    private String mountingProtocol;

    @SAGEAttribute(cvName = "line_query", termName = "organism")
    @SearchAttribute(key = "organism_txt", label = "Organism")
    private String organism;

    @SAGEAttribute(cvName = "line", termName = "genotype")
    @SearchAttribute(key = "genotype_txt", label = "Genotype")
    private String genotype;

    @SAGEAttribute(cvName = "line", termName = "flycore_id")
    @SearchAttribute(key = "flycore_id_i", label = "Fly Core Id")
    private Integer flycoreId;

    @SAGEAttribute(cvName = "light_imagery", termName = "imaging_project")
    @SearchAttribute(key = "img_proj_txt", label = "Imaging Project")
    private String imagingProject;

    @SAGEAttribute(cvName = "light_imagery", termName = "driver")
    @SearchAttribute(key = "driver_txt", label = "Driver")
    private String driver;

    @SAGEAttribute(cvName = "line", termName = "flycore_project")
    @SearchAttribute(key = "fcproj_txt", label = "Fly Core Project")
    private String flycoreProject;

    @SAGEAttribute(cvName = "line", termName = "flycore_lab")
    @SearchAttribute(key = "fclab_s", label = "Fly Core Lab Id")
    private String flycoreLabId;

    @SAGEAttribute(cvName = "light_imagery", termName = "family")
    @SearchAttribute(key = "family_txt", label = "Image Family")
    private String imageFamily;

    @SAGEAttribute(cvName = "image_query", termName = "line")
    @SearchAttribute(key = "line_txt", label = "Line")
    private String line;

    @SAGEAttribute(cvName = "light_imagery", termName = "vt_line")
    @SearchAttribute(key = "vtline_txt", label = "VT Line")
    private String vtLine;

    @SAGEAttribute(cvName = "light_imagery", termName = "publishing_name")
    @SearchAttribute(key = "pubname_txt", label = "Publishing Name")
    private String publishingName;

    @SAGEAttribute(cvName = "light_imagery", termName = "published_externally")
    @SearchAttribute(key = "pubext_b", label = "Published Externally")
    private String publishedExternally;

    @SAGEAttribute(cvName = "light_imagery", termName = "slide_code")
    @SearchAttribute(key = "slide_code_txt", label = "Slide Code")
    private String slideCode;

    @SAGEAttribute(cvName = "fly", termName = "cross_barcode")
    @SearchAttribute(key = "cross_barcode_txt", label = "Cross Barcode")
    private Integer crossBarcode;

    @SearchAttribute(key = "status_txt", label = "Status", facet = "status_s")
    private String status;

    @SearchAttribute(key = "sage_synced_b", label = "SAGE Synchronized", facet = "sage_synced_b")
    private boolean sageSynced = false;

    @SearchAttribute(key = "compression_txt", label = "Compression Type")
    private String compressionType;

    @SAGEAttribute(cvName = "image_query", termName = "create_date")
    @SearchAttribute(key = "tmog_dt", label = "TMOG Date")
    private Date tmogDate;

    @SearchAttribute(key = "completion_dt", label = "Completion Date")
    private Date completionDate;

    @SearchAttribute(key = "usage_bytes_l", label = "Disk Space Usage (Bytes)")
    private Long diskSpaceUsage;

    private List<ObjectiveSample> objectiveSamples = new ArrayList<>();

    public String getDataSet() {
        return dataSet;
    }

    public void setDataSet(String dataSet) {
        this.dataSet = dataSet;
    }

    public String getSlideCode() {
        return slideCode;
    }

    public void setSlideCode(String slideCode) {
        this.slideCode = slideCode;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getEffector() {
        return effector;
    }

    public void setEffector(String effector) {
        this.effector = effector;
    }

    public String getFlycoreAlias() {
        return flycoreAlias;
    }

    public void setFlycoreAlias(String flycoreAlias) {
        this.flycoreAlias = flycoreAlias;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getMountingProtocol() {
        return mountingProtocol;
    }

    public void setMountingProtocol(String mountingProtocol) {
        this.mountingProtocol = mountingProtocol;
    }

    public String getOrganism() {
        return organism;
    }

    public void setOrganism(String organism) {
        this.organism = organism;
    }

    public String getGenotype() {
        return genotype;
    }

    public void setGenotype(String genotype) {
        this.genotype = genotype;
    }

    public Integer getFlycoreId() {
        return flycoreId;
    }

    public void setFlycoreId(Integer flycoreId) {
        this.flycoreId = flycoreId;
    }

    public String getImagingProject() {
        return imagingProject;
    }

    public void setImagingProject(String imagingProject) {
        this.imagingProject = imagingProject;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getFlycoreProject() {
        return flycoreProject;
    }

    public void setFlycoreProject(String flycoreProject) {
        this.flycoreProject = flycoreProject;
    }

    public String getFlycoreLabId() {
        return flycoreLabId;
    }

    public void setFlycoreLabId(String flycoreLabId) {
        this.flycoreLabId = flycoreLabId;
    }

    public String getImageFamily() {
        return imageFamily;
    }

    public void setImageFamily(String imageFamily) {
        this.imageFamily = imageFamily;
    }

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
    }

    public String getVtLine() {
        return vtLine;
    }

    public void setVtLine(String vtLine) {
        this.vtLine = vtLine;
    }

    public String getPublishingName() {
        return publishingName;
    }

    public void setPublishingName(String publishingName) {
        this.publishingName = publishingName;
    }

    public String getPublishedExternally() {
        return publishedExternally;
    }

    public void setPublishedExternally(String publishedExternally) {
        this.publishedExternally = publishedExternally;
    }

    public Integer getCrossBarcode() {
        return crossBarcode;
    }

    public void setCrossBarcode(Integer crossBarcode) {
        this.crossBarcode = crossBarcode;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(String compressionType) {
        this.compressionType = compressionType;
    }

    public Date getTmogDate() {
        return tmogDate;
    }

    public void setTmogDate(Date tmogDate) {
        this.tmogDate = tmogDate;
    }

    public Date getCompletionDate() {
        return completionDate;
    }

    public void setCompletionDate(Date completionDate) {
        this.completionDate = completionDate;
    }

    public boolean isSageSynced() {
        return sageSynced;
    }

    public void setSageSynced(boolean sageSynced) {
        this.sageSynced = sageSynced;
    }

    public Long getDiskSpaceUsage() {
        return diskSpaceUsage;
    }

    public void setDiskSpaceUsage(Long diskSpaceUsage) {
        this.diskSpaceUsage = diskSpaceUsage;
    }

    public List<ObjectiveSample> getObjectiveSamples() {
        return objectiveSamples;
    }

    public void setObjectiveSamples(List<ObjectiveSample> objectiveSamples) {
        this.objectiveSamples = objectiveSamples;
    }

    public Optional<ObjectiveSample> lookupObjective(String objectiveName) {
        Optional<ObjectiveSample> objective = Optional.empty();
        if (CollectionUtils.isNotEmpty(objectiveSamples)) {
            objective = objectiveSamples.stream()
                    .filter(o -> StringUtils.isBlank(objectiveName) && StringUtils.isBlank(o.getObjective())
                            || o.getObjective().equals(objectiveName))
                    .findFirst();
        }
        return objective;
    }

    public Optional<IndexedReference<ObjectiveSample, Integer>> lookupIndexedObjective(String objectiveName) {
        Optional<IndexedReference<ObjectiveSample, Integer>> objective;
        if (CollectionUtils.isNotEmpty(objectiveSamples)) {
            objective = IndexedReference.indexListContent(objectiveSamples, (pos, os) -> new IndexedReference<>(os, pos))
                    .filter(positionalReference -> (StringUtils.isBlank(objectiveName) && StringUtils.isBlank(positionalReference.getReference().getObjective())
                                    || positionalReference.getReference().getObjective().equals(objectiveName)))
                    .findFirst();
        } else {
            objective = Optional.empty();
        }
        return objective;
    }

    public void addObjective(ObjectiveSample objective) {
        if (objectiveSamples == null) {
            objectiveSamples  = new ArrayList<>();
        }
        objective.setParent(this);
        objectiveSamples.add(objective);
    }
}
