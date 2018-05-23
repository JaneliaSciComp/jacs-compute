package org.janelia.jacs2.asyncservice.utils;

import org.janelia.jacs2.dataservice.nodes.FileStorePath;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Unit tests for the FileStorePath parser. 
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class FileStorePathTest {

    private static final String USER_NAME = "itom10";

    @Test
    public void testFileStorePath() {
        String path = "/groups/jacs/jacsDev/devstore/itom10/Sample/678/507/2396503392529678507/";
        FileStorePath f = FileStorePath.parseFilepath(path);
        Assert.assertEquals(path, f.getFilepath());
        Assert.assertEquals("/groups/jacs/jacsDev/devstore", f.getStorePath());
        Assert.assertEquals("itom10", f.getUsername());
        Assert.assertEquals("Sample", f.getType());
        Assert.assertEquals(2396503392529678507L, f.getId().longValue());
        Assert.assertEquals("", f.getRest());
    }

    @Test
    public void testFileStorePathWithRest() {
        String path = "/groups/jacs/jacsDev/devstore/itom10/Post/209/771/2396503817165209771/JRC_IS44397_20170310_54_F3-ventral_nerve_cord-UAS_Chrimson_Venus_X_0070_all.mp4";
        FileStorePath f = FileStorePath.parseFilepath(path);
        Assert.assertEquals(path, f.getFilepath());
        Assert.assertEquals("/groups/jacs/jacsDev/devstore", f.getStorePath());
        Assert.assertEquals("itom10", f.getUsername());
        Assert.assertEquals("Post", f.getType());
        Assert.assertEquals(2396503817165209771L, f.getId().longValue());
        Assert.assertEquals("JRC_IS44397_20170310_54_F3-ventral_nerve_cord-UAS_Chrimson_Venus_X_0070_all.mp4", f.getRest());
    }

    @Test
    public void testFileStorePathWithRestStripping() {
        String path = "/groups/jacs/jacsDev/devstore/itom10/Post/209/771/2396503817165209771/JRC_IS44397_20170310_54_F3-ventral_nerve_cord-UAS_Chrimson_Venus_X_0070_all.mp4";
        FileStorePath f = FileStorePath.parseFilepath(path);
        Assert.assertEquals("/groups/jacs/jacsDev/devstore/itom10/Post/209/771/2396503817165209771", f.getFilepath(true));
        Assert.assertEquals("/groups/jacs/jacsDev/devstore", f.getStorePath());
        Assert.assertEquals("itom10", f.getUsername());
        Assert.assertEquals("Post", f.getType());
        Assert.assertEquals(2396503817165209771L, f.getId().longValue());
        Assert.assertEquals("JRC_IS44397_20170310_54_F3-ventral_nerve_cord-UAS_Chrimson_Venus_X_0070_all.mp4", f.getRest());
    }

    @Test
    public void testFileStorePathHash() {
        String path1 = "/groups/jacs/jacsDev/devstore/itom10/Post/209/771/2396503817165209771/JRC_IS44397_20170310_54_F3-ventral_nerve_cord-UAS_Chrimson_Venus_X_0070_all.mp4";
        String path2 = "/groups/jacs/jacsDev/devstore/itom10/Post/209/771/2396503817165209771/JRC_IS44397_20170310_54_F3-brain-UAS_Chrimson_Venus_X_0070_signal.png";
        Set<String> set = new HashSet<>();
        set.add(FileStorePath.parseFilepath(path1).getFilepath());
        set.add(FileStorePath.parseFilepath(path2).getFilepath());
        Assert.assertEquals(2, set.size());
    }
    
    @Test
    public void testFileStorePathStrippedHash() {
        String path1 = "/groups/jacs/jacsDev/devstore/itom10/Post/209/771/2396503817165209771/JRC_IS44397_20170310_54_F3-ventral_nerve_cord-UAS_Chrimson_Venus_X_0070_all.mp4";
        String path2 = "/groups/jacs/jacsDev/devstore/itom10/Post/209/771/2396503817165209771/JRC_IS44397_20170310_54_F3-brain-UAS_Chrimson_Venus_X_0070_signal.png";
        Set<String> set = new HashSet<>();
        set.add(FileStorePath.parseFilepath(path1).getFilepath(true));
        set.add(FileStorePath.parseFilepath(path2).getFilepath(true));
        Assert.assertEquals(1, set.size());
    }
    
    @Test
    public void testFileStorePathInvalid() {
        boolean caught = false;
        try {
            String path = "/groups/jacs/jacsDev/devstore/itom10/Post/209/771/";
            FileStorePath fps = FileStorePath.parseFilepath(path);
            System.out.println(fps);
        }
        catch (IllegalArgumentException e) {
            caught = true;
        }
        Assert.assertTrue(caught);
    }
    
    @Test
    public void testFileStorePathStrippedToString() {
        String path = "/groups/jacs/jacsDev/devstore/itom10/Post/209/771/2396503817165209771/JRC_IS44397_20170310_54_F3-ventral_nerve_cord-UAS_Chrimson_Venus_X_0070_all.mp4";
        FileStorePath f = FileStorePath.parseFilepath(path);
        Assert.assertEquals("/groups/jacs/jacsDev/devstore/itom10/Post/209/771/2396503817165209771", f.getFilepath(true));
    }
    
    @Test
    public void testOwnerChange() {
        String path = "/groups/jacs/jacsDev/devstore/itom10/Sample/678/507/2396503392529678507/";
        FileStorePath f = FileStorePath.parseFilepath(path);
        Assert.assertEquals("itom10", f.getUsername());
        FileStorePath g = f.withChangedOwner("asoy");
        Assert.assertEquals("itom10", f.getUsername());
        Assert.assertEquals("asoy", g.getUsername());
        // The final trailing slash will get stripped
        Assert.assertEquals(path.replaceFirst(USER_NAME, "asoy"), g.getFilepath()+"/");
    }
    
}
