package org.janelia.jacs2.asyncservice.lvtservices;

class OctreeResult {
    private String basePath;
    private int levels;

    public String getBasePath() {
        return basePath;
    }

    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    public int getLevels() {
        return levels;
    }

    public void setLevels(int levels) {
        this.levels = levels;
    }
}
