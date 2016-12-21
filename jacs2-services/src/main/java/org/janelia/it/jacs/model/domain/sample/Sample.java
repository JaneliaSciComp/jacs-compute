package org.janelia.it.jacs.model.domain.sample;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.commons.collections4.CollectionUtils;
import org.janelia.it.jacs.model.domain.AbstractDomainObject;
import org.janelia.it.jacs.model.domain.support.MongoMapping;
import org.janelia.jacs2.utils.ISODateDeserializer;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * All the processing results of a particular specimen. Uniqueness of a Sample is determined by a combination
 * of data set and slide code. A single sample may include many LSMs. For example, it may include images taken
 * at multiple objectives (e.g. 20x/63x), of different anatomical areas (e.g. Brain/VNC), and of different
 * tile regions which are stitched together.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@MongoMapping(collectionName="sample", label="Sample")
public class Sample extends AbstractDomainObject {
    private String age;
    private String dataSet;
    private String effector;
    private String flycoreAlias;
    private String gender;
    private String line;
    private String slideCode;
    private Integer crossBarcode;
    private String status;
    private Boolean visited = false;
    private Boolean sageSynced = false;
    private String compressionType;
    @JsonDeserialize(using = ISODateDeserializer.class)
    private Date tmogDate;
    @JsonDeserialize(using = ISODateDeserializer.class)
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

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
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
                    .filter(o -> objectiveName == null && o.getObjective() == null || objectiveName.equals(o.getObjective()))
                    .findFirst();
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