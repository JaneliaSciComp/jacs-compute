package org.janelia.jacs2.asyncservice.imagesearch;

import java.util.Comparator;
import java.util.List;

class CDScoreUtils {
    static long calculateNegativeScore(Long gradientAreaGap, Long highExpressionArea) {
        if (gradientAreaGap != null && highExpressionArea != null) {
            return gradientAreaGap + highExpressionArea / 3;
        } else if (gradientAreaGap != null) {
            return gradientAreaGap;
        } else if (highExpressionArea != null) {
            return highExpressionArea / 3;
        } else {
            return -1;
        }
    }

    static double calculateNormalizedScore(Long gradientAreaGap, Long highExpressionArea, long maxNegativeScore, long pixelMatch, double pixelMatchRatio, long maxPixelMatch) {
        if (pixelMatch == 0 || pixelMatchRatio == 0 || maxNegativeScore == -1) {
            return pixelMatch;
        } else {
            long negativeScore = calculateNegativeScore(gradientAreaGap, highExpressionArea);
            if (negativeScore == -1) {
                return pixelMatch;
            } else {
                double normalizedNegativeScore = (double) negativeScore / maxNegativeScore;
                double boundedNegativeScore = Math.min(Math.max(normalizedNegativeScore * 2.5, 0.002), 1.);
                return (double) pixelMatch / (double) maxPixelMatch / boundedNegativeScore * 100;
            }
        }
    }

    static void sortCDSResults(List<CDSMatchResult> cdsResults) {
        Comparator<CDSMatchResult> csrComp = (csr1, csr2) -> {
            if (csr1.getNormalizedScore() != null && csr2.getNormalizedScore() != null) {
                return Comparator.comparingDouble(CDSMatchResult::getNormalizedScore)
                        .compare(csr1, csr2)
                        ;
            } else if (csr1.getNormalizedScore() == null && csr2.getNormalizedScore() == null) {
                return Comparator.comparingInt(CDSMatchResult::getMatchingPixels)
                        .compare(csr1, csr2)
                        ;
            } else if (csr1.getNormalizedScore() == null) {
                // null gap scores should be at the beginning
                return -1;
            } else {
                return 1;
            }
        };
        cdsResults.sort(csrComp.reversed());
    }

}
