package org.janelia.jacs2.asyncservice.imageservices;

import java.util.ArrayList;
import java.util.List;

public class BasicMIPsAndMoviesResult {
    private String resultsDir;
    private List<String> fileList = new ArrayList<>();

    public String getResultsDir() {
        return resultsDir;
    }

    public void setResultsDir(String resultsDir) {
        this.resultsDir = resultsDir;
    }

    public List<String> getFileList() {
        return fileList;
    }

    public void setFileList(List<String> fileList) {
        this.fileList = fileList;
    }

    public void addFile(String f) {
        fileList.add(f);
    }
}
