package org.janelia.jacs2.asyncservice.imagesearch;

import java.util.ArrayList;
import java.util.List;

/**
 * The ranked results of a Color Depth Search.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class ColorDepthSearchResults {

    private List<ColorDepthSearchResult> resultList = new ArrayList<>();

    public List<ColorDepthSearchResult> getResultList() {
        return resultList;
    }

    public static class ColorDepthSearchResult {

        private Float score;
        private String filename;

        public ColorDepthSearchResult(Float score, String filename) {
            this.score = score;
            this.filename = filename;
        }

        public float getScore() {
            return score;
        }

        public String getFilename() {
            return filename;
        }
    }
}
