package org.janelia.it.jacs.model.domain.enums;

public enum ObjectiveType {
    _10X("10x"),
    _20X("20x"),
    _25X("25x"),
    _40X("40x"),
    _63X("63x");

    private String objectiveType;

    ObjectiveType(String objectiveType) {
        this.objectiveType = objectiveType;
    }

    public String getObjectiveType() {
        return objectiveType;
    }
}
