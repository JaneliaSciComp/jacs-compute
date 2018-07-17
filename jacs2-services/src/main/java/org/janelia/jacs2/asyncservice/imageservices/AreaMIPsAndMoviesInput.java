package org.janelia.jacs2.asyncservice.imageservices;

public class AreaMIPsAndMoviesInput {
    private String filepath;
    private String outputPrefix;
    private String chanspec;
    private String area;
    private String key;

    public String getFilepath() {
        return filepath;
    }

    public void setFilepath(String filepath) {
        this.filepath = filepath;
    }

    public String getOutputPrefix() {
        return outputPrefix;
    }

    public void setOutputPrefix(String outputPrefix) {
        this.outputPrefix = outputPrefix;
    }

    public String getChanspec() {
        return chanspec;
    }

    public void setChanspec(String chanspec) {
        this.chanspec = chanspec;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
