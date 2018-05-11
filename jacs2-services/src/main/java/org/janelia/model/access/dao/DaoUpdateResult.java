package org.janelia.model.access.dao;

public class DaoUpdateResult {
    private final long entitiesFound;
    private final long entitiesAffected;

    public DaoUpdateResult(long entitiesFound, long entitiesAffected) {
        this.entitiesFound = entitiesFound;
        this.entitiesAffected = entitiesAffected;
    }

    public long getEntitiesFound() {
        return entitiesFound;
    }

    public long getEntitiesAffected() {
        return entitiesAffected;
    }
}
