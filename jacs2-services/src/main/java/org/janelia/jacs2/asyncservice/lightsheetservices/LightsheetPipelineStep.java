package org.janelia.jacs2.asyncservice.lightsheetservices;

public enum LightsheetPipelineStep {
    clusterCS(true, 8),
    clusterFR(true, 2),
    clusterMF(true, 2),
    clusterPT(true, 2),
    clusterTF(true, 2),
    localAP(false, 32),
    localEC(false, 32),
    generateMiniStacks(false, 32);

    private boolean needsMoreThanOneJob;
    private int recommendedSlots;

    LightsheetPipelineStep(boolean needsMoreThanOneJob, int recommendedSlots) {
        this.needsMoreThanOneJob = needsMoreThanOneJob;
        this.recommendedSlots = recommendedSlots;
    }

    public boolean requiresMoreThanOneJob() {
        return needsMoreThanOneJob;
    }

    public boolean needsOnlyOneJob() {
        return !needsMoreThanOneJob;
    }

    public int getRecommendedSlots() {
        return recommendedSlots;
    }
}
