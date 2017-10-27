package org.janelia.model.jacs2;

public interface BaseEntity {
    default String getEntityName() {
        return getClass().getSimpleName();
    };
}
