package org.janelia.jacs2.asyncservice.imageservices;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class MIPsAndMoviesResult {
    private final String fileInput;
    private String resultsDir;
    private List<String> fileList = new ArrayList<>();

    @JsonCreator
    public MIPsAndMoviesResult(@JsonProperty("fileInput") String fileInput) {
        this.fileInput = fileInput;
    }

    public String getFileInput() {
        return fileInput;
    }

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

    public MIPsAndMoviesResult addFile(String f) {
        fileList.add(f);
        return this;
    }
}
