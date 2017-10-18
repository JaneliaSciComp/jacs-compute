package org.janelia.jacs2.asyncservice.alignservices;

import java.util.ArrayList;
import java.util.List;

public class CMTKAlignmentResultFiles {
    private String resultDir;
    private List<String> reformattedFiles = new ArrayList<>();
    private String affineRegistrationResultsDir;
    private String warpRegistrationResultsDir;

    public String getResultDir() {
        return resultDir;
    }

    public void setResultDir(String resultDir) {
        this.resultDir = resultDir;
    }

    public List<String> getReformattedFiles() {
        return reformattedFiles;
    }

    public void setReformattedFiles(List<String> reformattedFiles) {
        this.reformattedFiles = reformattedFiles;
    }

    public void addReformattedFile(String reformattedFile) {
        if (this.reformattedFiles == null) this.reformattedFiles = new ArrayList<>();
        if (!reformattedFiles.contains(reformattedFile)) {
            this.reformattedFiles.add(reformattedFile);
        }
    }

    public String getAffineRegistrationResultsDir() {
        return affineRegistrationResultsDir;
    }

    public void setAffineRegistrationResultsDir(String affineRegistrationResultsDir) {
        this.affineRegistrationResultsDir = affineRegistrationResultsDir;
    }

    public String getWarpRegistrationResultsDir() {
        return warpRegistrationResultsDir;
    }

    public void setWarpRegistrationResultsDir(String warpRegistrationResultsDir) {
        this.warpRegistrationResultsDir = warpRegistrationResultsDir;
    }
}
