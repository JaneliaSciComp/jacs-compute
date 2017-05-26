package org.janelia.jacs2.asyncservice.imageservices;

import org.janelia.jacs2.model.jacsservice.annotation.JacsServiceResultType;

import java.io.File;

@JacsServiceResultType
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
