package org.janelia.model.jacs2.domain.codedValues;

public enum MergeAlgorithm {

    FLYLIGHT("FlyLight LSM Pair Merge (3/2 to 4 channels)"),
    FLYLIGHT_ORDERED("FlyLight LSM Pair Merge (Consistent Ordering)"),
    DISTORTION_CORRECTION_MERGE("FlyLight LSM Pair Merge with Distortion Correction (Consistent Ordering)");

    private String name;

    MergeAlgorithm(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
