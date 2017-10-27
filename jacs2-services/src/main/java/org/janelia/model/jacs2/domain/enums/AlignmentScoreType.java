package org.janelia.model.jacs2.domain.enums;

/**
 * Different types of image alignment scores.
 */
public enum AlignmentScoreType {

    Inconsistency("Alignment Inconsistency Score"),
    InconsistencyByRegion("Alignment Inconsistency Scores"),
    NormalizedCrossCorrelation("Normalized Cross Correlation Score"),
    ModelViolation("Model Violation Score"),
    Qi("Alignment Qi Score"),
    QiByRegion("Alignment Qi Scores"),
    OverlapCoefficient("Overlap Coefficient"),
    ObjectPearsonCoefficient("Object Pearson Coefficient"),
    OtsunaObjectPearsonCoefficient("Otsuna Object Pearson Coefficient");

    private final String label;

    AlignmentScoreType(String label) {
        this.label = label;
    }

    public String getLabel() {
        return label;
    }
}
