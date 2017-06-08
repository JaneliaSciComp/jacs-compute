package org.janelia.jacs2.asyncservice.alignservices;

import java.util.ArrayList;
import java.util.List;

public class AlignmentResultFiles {
    private String alignmentPropertiesFile;

    private String algorithm;
    private String resultDir;
    private List<String> alignedResultFiles = new ArrayList<>();
    private String scoresFile;
    private String rotationsFile;
    private String affineFile;
    private List<String> warpedFiles = new ArrayList<>();
    private String alignmentVerificationMovie;

    public String getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }

    public String getResultDir() {
        return resultDir;
    }

    public void setResultDir(String resultDir) {
        this.resultDir = resultDir;
    }

    public String getAlignmentPropertiesFile() {
        return alignmentPropertiesFile;
    }

    public void setAlignmentPropertiesFile(String alignmentPropertiesFile) {
        this.alignmentPropertiesFile = alignmentPropertiesFile;
    }

    public List<String> getAlignedResultFiles() {
        return alignedResultFiles;
    }

    public void setAlignedResultFiles(List<String> alignedResultFiles) {
        this.alignedResultFiles = alignedResultFiles;
    }

    public void addAlignedResultFiles(String alignedResultFile) {
        this.alignedResultFiles.add(alignedResultFile);
    }

    public String getScoresFile() {
        return scoresFile;
    }

    public void setScoresFile(String scoresFile) {
        this.scoresFile = scoresFile;
    }

    public String getRotationsFile() {
        return rotationsFile;
    }

    public void setRotationsFile(String rotationsFile) {
        this.rotationsFile = rotationsFile;
    }

    public String getAffineFile() {
        return affineFile;
    }

    public void setAffineFile(String affineFile) {
        this.affineFile = affineFile;
    }

    public List<String> getWarpedFiles() {
        return warpedFiles;
    }

    public void setWarpedFiles(List<String> warpedFiles) {
        this.warpedFiles = warpedFiles;
    }

    public String getAlignmentVerificationMovie() {
        return alignmentVerificationMovie;
    }

    public void setAlignmentVerificationMovie(String alignmentVerificationMovie) {
        this.alignmentVerificationMovie = alignmentVerificationMovie;
    }
}
