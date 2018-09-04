package org.janelia.jacs2.asyncservice.sample.aux;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * An anatomical area within a sample.
 * 
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class AnatomicalArea implements Serializable {

    private Long sampleId;
    private String objective;
    private String name;
    private Set<String> tileNames = new HashSet<>();
    private List<MergedLsmPair> mergedLsmPairs;
    private String stitchedFilepath;

    public AnatomicalArea(Long sampleId, String objective, String anatomicalArea) {
    	this.sampleId = sampleId;
    	this.objective = objective;
        this.name = anatomicalArea;
    }

    public Long getSampleId() {
        return sampleId;
    }

    public String getObjective() {
        return objective;
    }

    public void addTileName(String imageTileId) {
        tileNames.add(imageTileId);
    }

    public String getName() {
        return name;
    }

    public Set<String> getTileNames() {
        return tileNames;
    }

    public List<MergedLsmPair> getMergedLsmPairs() {
        return mergedLsmPairs;
    }

    public void setMergedLsmPairs(List<MergedLsmPair> mergedLsmPairs) {
        this.mergedLsmPairs = mergedLsmPairs;
    }

    public String getStitchedFilepath() {
        return stitchedFilepath;
    }

    public void setStitchedFilepath(String stitchedFilepath) {
        this.stitchedFilepath = stitchedFilepath;
    }

    @Override
    public String toString() {
        return "AnatomicalArea{" +
                "name='" + name + '\'' +
                ", tiles=" + tileNames +
                ", filepath=" + stitchedFilepath +
                ", mergedLsmPairs=" + mergedLsmPairs +
                '}';
    }

    /**
     * Reconstitute an AA from its c'tor parameters.
     */
    public static List<AnatomicalArea> reconstructBasicAnatomicalAreasFromMemento(String areasString) {
        String[] areasArr = areasString.split("\n");
        List<AnatomicalArea> rtnVal = new ArrayList<>();
        for (String area: areasArr) {
            String[] areaParams = area.split(",");
            AnatomicalArea aa = new AnatomicalArea(Long.parseLong(areaParams[2]), areaParams[1], areaParams[0]);
            rtnVal.add(aa);
        }
        return rtnVal;
    }

}
