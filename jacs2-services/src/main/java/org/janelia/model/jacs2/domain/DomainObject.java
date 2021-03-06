package org.janelia.model.jacs2.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.janelia.model.jacs2.BaseEntity;
import org.janelia.model.jacs2.domain.interfaces.HasIdentifier;

import java.util.Date;
import java.util.Set;

/**
 * A domain object is anything stored at the top level of a collection.
 * It must have a GUID, a name, and user ownership/permissions.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
public interface DomainObject extends BaseEntity, HasIdentifier {
    @JsonIgnore
    String getEntityRefId();
    String getName();
    void setName(String name);
    String getOwnerName();
    String getOwnerKey();
    void setOwnerKey(String ownerKey);
    String getLockKey();
    void setLockKey(String lockKey);
    Date getLockTimestamp();
    void setLockTimestamp(Date lockTimestamp);
    Set<String> getReaders();
    void setReaders(Set<String> readers);
    void addReader(String reader);
    Set<String> getWriters();
    void setWriters(Set<String> writers);
    void addWriter(String writer);
    Date getCreationDate();
    void setCreationDate(Date creationDate);
    Date getUpdatedDate();
    void setUpdatedDate(Date updatedDate);
}
