package org.janelia.it.jacs.model.domain.sample;

import org.apache.commons.collections4.CollectionUtils;
import org.janelia.it.jacs.model.domain.AbstractDomainObject;
import org.janelia.it.jacs.model.domain.IndexedReference;
import org.janelia.it.jacs.model.domain.support.MongoMapping;
import org.janelia.it.jacs.model.domain.support.SAGEAttribute;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

/**
 * All the processing results of a particular specimen. Uniqueness of a Sample is determined by a combination
 * of data set and slide code. A single sample may include many LSMs. For example, it may include images taken
 * at multiple objectives (e.g. 20x/63x), of different anatomical areas (e.g. Brain/VNC), and of different
 * tile regions which are stitched together.
 */
@MongoMapping(collectionName="sample", label="Sample")
public class Sample extends AbstractDomainObject {
    @SAGEAttribute(cvName="light_imagery", termName="age")
    private String age;
    @SAGEAttribute(cvName="light_imagery", termName="data_set")
    private String dataSet;
    @SAGEAttribute(cvName="fly", termName="effector")
    private String effector;
    @SAGEAttribute(cvName="line", termName="flycore_alias")
    private String flycoreAlias;
    @SAGEAttribute(cvName="light_imagery", termName="gender")
    private String gender;
    @SAGEAttribute(cvName="light_imagery", termName="mounting_protocol")
    private String mountingProtocol;
    @SAGEAttribute(cvName="line_query", termName="organism")
    private String organism;
    @SAGEAttribute(cvName="line", termName="genotype")
    private String genotype;
    @SAGEAttribute(cvName="line", termName="flycore_id")
    private Integer flycoreId;
    @SAGEAttribute(cvName="light_imagery", termName="imaging_project")
    private String imagingProject;
    @SAGEAttribute(cvName="light_imagery", termName="driver")
    private String driver;
    @SAGEAttribute(cvName="line", termName="flycore_project")
    private String flycoreProject;
    @SAGEAttribute(cvName="line", termName="flycore_lab")
    private String flycoreLabId;
    @SAGEAttribute(cvName="light_imagery", termName="family")
    private String imageFamily;
    @SAGEAttribute(cvName="image_query", termName="line")
    private String line;
    @SAGEAttribute(cvName="light_imagery", termName="vt_line")
    private String vtLine;
    @SAGEAttribute(cvName="light_imagery", termName="publishing_name")
    private String publishingName;
    @SAGEAttribute(cvName="light_imagery", termName="published_externally")
    private String publishedExternally;
    @SAGEAttribute(cvName="light_imagery", termName="slide_code")
    private String slideCode;
    @SAGEAttribute(cvName="fly", termName="cross_barcode")
    private Integer crossBarcode;
    private String status;
    private boolean sageSynced = false;
    private String compressionType;
    @SAGEAttribute(cvName="image_query", termName="create_date")
    private Date tmogDate;
    private Date completionDate;
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
                    .filter(o -> objectiveName == null && o.getObjective() == null || o.getObjective().equals(objectiveName))
                    .findFirst();
        }
        return objective;
    }

    public Optional<IndexedReference<ObjectiveSample, Integer>> lookupObjectiveWithPos(String objectiveName) {
        Optional<IndexedReference<ObjectiveSample, Integer>> objective;
        if (CollectionUtils.isNotEmpty(objectiveSamples)) {
            objective = IndexedReference.indexListContent(objectiveSamples, (pos, os) -> new IndexedReference<>(os, pos))
                    .filter(positionalReference -> (objectiveName == null && positionalReference.getReference().getObjective() == null
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
