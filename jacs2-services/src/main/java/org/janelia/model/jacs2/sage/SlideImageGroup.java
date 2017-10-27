package org.janelia.model.jacs2.sage;

import org.apache.commons.lang3.StringUtils;
import org.janelia.model.jacs2.domain.sample.LSMImage;

import java.util.ArrayList;
import java.util.List;

/**
 * SlideImageGroup groups a list of lsms by the anatomical area.
 */
public class SlideImageGroup {

    private final String tag;
    private final List<LSMImage> images = new ArrayList<>();
    private String anatomicalArea;

    public SlideImageGroup(String tag, String anatomicalArea) {
        this.tag = tag;
        this.anatomicalArea = anatomicalArea;
    }

    public String getTag() {
        return tag;
    }

    public List<LSMImage> getImages() {
        return images;
    }

    public String getAnatomicalArea() {
        return anatomicalArea;
    }

    public void setAnatomicalArea(String anatomicalArea) {
        this.anatomicalArea = anatomicalArea;
    }

    public void addImage(LSMImage lsmImage) {
        images.add(lsmImage);
    }

    public int countTileSignalChannelsUsingChanSpec() {
        return images.stream()
                .map(lsm -> lsm.getChanSpec())
                .filter(chanSpec -> StringUtils.isNotBlank(chanSpec))
                .flatMap((String chanSpec) -> chanSpec.chars().mapToObj(i -> (char) i))
                .filter((Character chan) -> chan.equals('s'))
                .reduce(0, (totalSignalChannels, chan) -> totalSignalChannels++, (n1, n2) -> n1 + n2);
    }

    public int countTileSignalChannelsUsingNumChannels() {
        return images.stream()
                .map(lsm -> lsm.getNumChannels())
                .filter(numChannels -> numChannels != null && numChannels > 0)
                .map(numChannels -> numChannels -1)
                .reduce(0, (n1, n2) -> n1 + n2);
    }

    public int  countTileSignalChannels() {
        int nSignalChannels = countTileSignalChannelsUsingChanSpec();
        if (nSignalChannels > 0) {
            return nSignalChannels;
        } else {
            return countTileSignalChannelsUsingNumChannels();
        }
    }
}
