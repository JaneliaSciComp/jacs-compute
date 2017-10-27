package org.janelia.model.jacs2.domain.interfaces;

public interface HasIdentifier {
    Number getId();
    void setId(Number id);
    default boolean hasId() {
        return getId() != null;
    }
    default boolean sameId(Number anotherId) { return hasId() && anotherId != null && anotherId.toString().equals(getId().toString()); }
    default boolean notSameId(Number anotherId) { return !sameId(anotherId); }
}
