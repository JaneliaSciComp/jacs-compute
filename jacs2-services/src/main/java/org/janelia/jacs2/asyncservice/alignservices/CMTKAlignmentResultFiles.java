package org.janelia.jacs2.asyncservice.alignservices;

public class CMTKAlignmentResultFiles {
    private String resultDir;
    private String reformattedFile;
    private String affineRegistrationResultsDir;
    private String warpRegistrationResultsDir;

    public String getResultDir() {
        return resultDir;
    }

    public void setResultDir(String resultDir) {
        this.resultDir = resultDir;
    }

    public String getReformattedFile() {
        return reformattedFile;
    }

    public void setReformattedFile(String reformattedFile) {
        this.reformattedFile = reformattedFile;
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
