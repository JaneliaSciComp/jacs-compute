package org.janelia.jacs2.asyncservice.sampleprocessing;

import org.janelia.jacs2.model.jacsservice.annotation.JacsServiceResultType;

import java.util.ArrayList;
import java.util.List;

@JacsServiceResultType
public class SampleImageMIPsFile {
    private SampleImageFile sampleImageFile;
    private String mipsResultsDir;
    private List<String> mips = new ArrayList<>();

    public SampleImageFile getSampleImageFile() {
        return sampleImageFile;
    }

    public void setSampleImageFile(SampleImageFile sampleImageFile) {
        this.sampleImageFile = sampleImageFile;
    }

    public String getMipsResultsDir() {
        return mipsResultsDir;
    }

    public void setMipsResultsDir(String mipsResultsDir) {
        this.mipsResultsDir = mipsResultsDir;
    }

    public List<String> getMips() {
        return mips;
    }

    public void setMips(List<String> mips) {
        this.mips = mips;
    }

    public void addMipFile(String mip) {
        this.mips.add(mip);
    }
}
