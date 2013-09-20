package org.janelia.it.FlyWorkstation.octree;

import static org.junit.Assert.*;

import java.io.File;
import java.net.MalformedURLException;

import org.janelia.it.FlyWorkstation.gui.slice_viewer.SharedVolumeImage;
import org.janelia.it.FlyWorkstation.gui.slice_viewer.Subvolume;
import org.junit.Test;

public class TestExtractSubvolume {

    @Test
    public void testExtractUpperRightBackValue() {
        // I see a neurite going from one of these points to another in data set AAV 4/25/2013
        ZoomLevel zoomLevel = new ZoomLevel(0);
        ZoomedVoxelIndex v1 = new ZoomedVoxelIndex(zoomLevel, 29952, 24869, 1243); // upper right back corner
        ZoomedVoxelIndex v2 = new ZoomedVoxelIndex(zoomLevel, 29753, 25609, 1233); // lower left front corner
        SharedVolumeImage wholeImage = new SharedVolumeImage();
        // TODO - this only works on Windows with mousebrainmicro drive mounted as M:
        String octreeFolder = "M:/render/2013-04-25-AAV";
        try {
            wholeImage.loadURL(new File(octreeFolder).toURI().toURL());
        } catch (MalformedURLException e) {
            e.printStackTrace();
            fail("Error opening octree directory "+octreeFolder);
        }
        Subvolume subvolume = new Subvolume(v1, v2, wholeImage);
        assertEquals(25281, subvolume.getIntensityGlobal(v1, 0));
        assertEquals(25903, subvolume.getIntensityGlobal(v2, 0));
    }

}
