package org.janelia.jacs2.asyncservice.imagesearch;

import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CDScoreUtils {
    private static final int HIGH_EXPRESSION_FACTOR = 2;
    private static final double LOW_NORMALIZED_NEGATIVE_SCORE = 0.002;
    private static final double HIGH_NORMALIZED_NEGATIVE_SCORE = 1.;

    static long calculateNegativeScore(Long gradientAreaGap, Long highExpressionArea) {
        if (gradientAreaGap != null && highExpressionArea != null) {
            return gradientAreaGap + highExpressionArea / HIGH_EXPRESSION_FACTOR;
        } else if (gradientAreaGap != null) {
            return gradientAreaGap;
        } else if (highExpressionArea != null) {
            return highExpressionArea / HIGH_EXPRESSION_FACTOR;
        } else {
            return -1;
        }
    }

    static double calculateNormalizedScore(Integer pixelMatch,
                                           Long gradientAreaGap,
                                           Long highExpressionArea,
                                           Integer maxPixelMatch,
                                           Long maxNegativeScore) {
        if (pixelMatch == null || pixelMatch == 0) {
            return 0;
        } else if (maxPixelMatch == null || maxPixelMatch == 0 || maxNegativeScore == null || maxNegativeScore < 0) {
            return pixelMatch;
        } else {
            double negativeScore = calculateNegativeScore(gradientAreaGap, highExpressionArea);
            if (negativeScore == -1) {
                return pixelMatch;
            }
            double normalizedNegativeScore = negativeScore / maxNegativeScore;
            double boundedNegativeScore = Math.min(
                    Math.max(normalizedNegativeScore * 2.5, LOW_NORMALIZED_NEGATIVE_SCORE),
                    HIGH_NORMALIZED_NEGATIVE_SCORE
            );
            return (double)pixelMatch / (double)maxPixelMatch / boundedNegativeScore * 100;
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
