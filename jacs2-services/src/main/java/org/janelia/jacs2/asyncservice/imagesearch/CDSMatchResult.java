package org.janelia.jacs2.asyncservice.imagesearch;

public class CDSMatchResult extends AbstractCDMMetadata {
    private int matchingPixels;
    private double matchingRatio;
    private Long gradientAreaGap;
    private Long highExpressionArea;
    private Double normalizedScore;

    public int getMatchingPixels() {
        return matchingPixels;
    }

    public void setMatchingPixels(int matchingPixels) {
        this.matchingPixels = matchingPixels;
    }

    public double getMatchingRatio() {
        return matchingRatio;
    }

    public void setMatchingRatio(double matchingRatio) {
        this.matchingRatio = matchingRatio;
    }

    public Long getGradientAreaGap() {
        return gradientAreaGap;
    }

    public void setGradientAreaGap(Long gradientAreaGap) {
        this.gradientAreaGap = gradientAreaGap;
    }

    public Long getHighExpressionArea() {
        return highExpressionArea;
    }

    public void setHighExpressionArea(Long highExpressionArea) {
        this.highExpressionArea = highExpressionArea;
    }

    public Double getNormalizedScore() {
        return normalizedScore;
    }

    public void setNormalizedScore(Double normalizedScore) {
        this.normalizedScore = normalizedScore;
    }
}
