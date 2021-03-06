package org.janelia.model.jacs2.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.janelia.model.domain.entityannotations.EntityId;
import org.janelia.model.jacs2.DomainModelUtils;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * Every top-level "domain object" we store in MongoDB has a core set of attributes
 * which allow for identification (id/name) and permissions (owner/readers/writers)
 * as well as safe-updates with updatedDate.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
public abstract class AbstractDomainObject implements DomainObject {
    @EntityId
    private Number id;
    private String name;
    private String ownerKey;
    private String lockKey;
    private Date lockTimestamp;
    private Set<String> readers = new HashSet<>();
    private Set<String> writers = new HashSet<>();
    private Date creationDate = new Date();
    private Date updatedDate;

    @JsonIgnore
    @Override
    public String getEntityName() {
        return this.getClass().getSimpleName();
    }

    @JsonIgnore
    @Override
    public String getEntityRefId() {
        return getEntityName() + "#" + (hasId() ? getId() : "");
    }

    @JsonIgnore
    @Override
    public String getOwnerName() {
        return DomainModelUtils.getNameFromSubjectKey(ownerKey);
    }

    @Override
    public Number getId() {
        return id;
    }

    @Override
    public void setId(Number id) {
        this.id = id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getOwnerKey() {
        return ownerKey;
    }

    @Override
    public void setOwnerKey(String ownerKey) {
        this.ownerKey = ownerKey;
    }

    @Override
    public String getLockKey() {
        return lockKey;
    }

    @Override
    public void setLockKey(String lockKey) {
        this.lockKey = lockKey;
    }

    @Override
    public Date getLockTimestamp() {
        return lockTimestamp;
    }

    @Override
    public void setLockTimestamp(Date lockTimestamp) {
        this.lockTimestamp = lockTimestamp;
    }

    @Override
    public Set<String> getReaders() {
        return readers;
    }

    @Override
    public void setReaders(Set<String> readers) {
        Preconditions.checkArgument(readers != null, "Readers property cannot be null");
        this.readers = readers;
    }

    @Override
    public void addReader(String reader) {
        if (StringUtils.isNotBlank(reader)) readers.add(reader);
    }

    @Override
    public Set<String> getWriters() {
        return writers;
    }

    @Override
    public void setWriters(Set<String> writers) {
        Preconditions.checkArgument(writers != null, "Writers property cannot be null");
        this.writers = writers;
    }

    @Override
    public void addWriter(String writer) {
        if (StringUtils.isNotBlank(writer)) writers.add(writer);
    }

    @Override
    public Date getCreationDate() {
        return creationDate;
    }

    @Override
    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    @Override
    public Date getUpdatedDate() {
        return updatedDate;
    }

    @Override
    public void setUpdatedDate(Date updatedDate) {
        this.updatedDate = updatedDate;
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this, false);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj, false);
    }

    @Override
    public String toString() {
        return getEntityName() + "#" + id;
    }
}
