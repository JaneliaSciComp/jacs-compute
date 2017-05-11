package org.janelia.jacs2.asyncservice.imageservices;

import java.io.File;

public class StitchAndBlendResult {
    private File stitchedImageInfoFile;
    private File stitchedFile;

    public File getStitchedImageInfoFile() {
        return stitchedImageInfoFile;
    }

    public void setStitchedImageInfoFile(File stitchedImageInfoFile) {
        this.stitchedImageInfoFile = stitchedImageInfoFile;
    }

    public File getStitchedFile() {
        return stitchedFile;
    }

    public void setStitchedFile(File stitchedFile) {
        this.stitchedFile = stitchedFile;
    }
}
