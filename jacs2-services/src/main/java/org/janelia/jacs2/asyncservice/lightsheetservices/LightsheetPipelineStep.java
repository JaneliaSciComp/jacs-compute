package org.janelia.jacs2.asyncservice.lightsheetservices;

public enum LightsheetPipelineStep {
    clusterCS(true, 6),
    clusterFR(true, 2),
    clusterMF(true, 2),
    clusterPT(true, 2),
    clusterTF(true, 2),
    localAP(false, 4),
    localEC(false, 8),
    generateMiniStacks(false, 32);

    private boolean canSplitJob;
    private int recommendedSlots;

    LightsheetPipelineStep(boolean canSplitJob, int recommendedSlots) {
        this.canSplitJob = canSplitJob;
        this.recommendedSlots = recommendedSlots;
    }

    public boolean cannotSplitJob() {
        return !canSplitJob;
    }

    public int getRecommendedSlots() {
        return recommendedSlots;
    }
}
