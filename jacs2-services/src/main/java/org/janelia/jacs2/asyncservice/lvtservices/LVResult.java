package org.janelia.jacs2.asyncservice.lvtservices;

class LVResult {
    private String baseTiffPath;
    private String baseKtxPath;
    private int levels;

    public String getBaseTiffPath() {
        return baseTiffPath;
    }

    public void setBaseTiffPath(String baseTiffPath) {
        this.baseTiffPath = baseTiffPath;
    }

    public String getBaseKtxPath() {
        return baseKtxPath;
    }

    public void setBaseKtxPath(String baseKtxPath) {
        this.baseKtxPath = baseKtxPath;
    }

    public int getLevels() {
        return levels;
    }

    public void setLevels(int levels) {
        this.levels = levels;
    }
}
