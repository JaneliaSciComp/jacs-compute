package org.janelia.it.jacs.model.domain.codedValues;

public enum MergeAlgorithm {

    FLYLIGHT("FlyLight LSM Pair Merge (3/2 to 4 channels)"),
    FLYLIGHT_ORDERED("FlyLight LSM Pair Merge (Consistent Ordering)");

    private String name;

    MergeAlgorithm(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
