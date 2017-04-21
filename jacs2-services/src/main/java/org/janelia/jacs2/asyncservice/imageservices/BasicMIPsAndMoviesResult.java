package org.janelia.jacs2.asyncservice.imageservices;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class BasicMIPsAndMoviesResult {
    private String resultsDir;
    private List<File> fileList = new ArrayList<>();

    public String getResultsDir() {
        return resultsDir;
    }

    public void setResultsDir(String resultsDir) {
        this.resultsDir = resultsDir;
    }

    public List<File> getFileList() {
        return fileList;
    }

    public void setFileList(List<File> fileList) {
        this.fileList = fileList;
    }

    public void addFile(File f) {
        fileList.add(f);
    }
}
