package org.janelia.it.FlyWorkstation.gui.alignment_board_viewer.volume_builder;

import junit.framework.Assert;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: fosterl
 * Date: 3/25/13
 * Time: 11:26 PM
 *
 * Test for the renderables-builder base class facility methods.
 */
public class DownSamplerTest {

    private static final byte[] RAW_VOLUME = new byte[] {
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,1,0,1,0,1,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,1,0,1,0,1,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,1,0,1,0,1,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,1,0,1,0,1,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,1,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,1,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,1,0,1,0,1,0,1,0,0,
            0,0,0,1,0,1,0,0,0,1,0,1,0,1,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
    };

    private static final byte[] ONE_BYTE_VOLUME = new byte[] {
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,1,0,1,0,1,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,1,0,1,0,1,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,1,0,1,0,1,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,1,0,1,0,1,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,
            0,0,0,1,0,1,0,1,0,1,0,1,0,0,0,0,
            0,0,0,1,0,1,0,0,0,1,0,1,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
    };

    private static final byte[] CIRCLE_VOLUME = new byte[] {
        //  0       3       7      11    15

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,1,1,0,0,0,0,0,0,0,0,
            0,0,0,0,1,1,0,0,1,1,0,0,0,0,0,0,
            0,0,0,1,1,0,0,0,0,1,1,0,0,0,0,0,
            0,0,0,0,1,1,0,0,1,1,0,0,0,0,0,0,
            0,0,0,0,0,0,1,1,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,1,1,0,0,0,0,0,0,0,0,
            0,0,0,0,1,1,0,0,1,1,0,0,0,0,0,0,
            0,0,0,1,1,0,0,0,0,1,1,0,0,0,0,0,
            0,0,0,0,1,1,0,0,1,1,0,0,0,0,0,0,
            0,0,0,0,0,0,1,1,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,1,1,0,0,0,0,0,0,0,0,
            0,0,0,0,1,1,0,0,1,1,0,0,0,0,0,0,
            0,0,0,1,1,0,0,0,0,1,1,0,0,0,0,0,
            0,0,0,0,1,1,0,0,1,1,0,0,0,0,0,0,
            0,0,0,0,0,0,1,1,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,1,1,0,0,0,0,0,0,0,0,
            0,0,0,0,1,1,0,0,1,1,0,0,0,0,0,0,
            0,0,0,1,1,0,0,0,0,1,1,0,0,0,0,0,
            0,0,0,0,1,1,0,0,1,1,0,0,0,0,0,0,
            0,0,0,0,0,0,1,1,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,1,1,0,0,0,0,0,0,0,0,
            0,0,0,0,1,1,0,0,1,1,0,0,0,0,0,0,
            0,0,0,1,1,0,0,0,0,1,1,0,0,0,0,0,
            0,0,0,0,1,1,0,0,1,1,0,0,0,0,0,0,
            0,0,0,0,0,0,1,1,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,1,1,0,0,0,0,0,0,0,0,
            0,0,0,0,1,1,0,0,1,1,0,0,0,0,0,0,
            0,0,0,1,1,0,0,0,0,1,1,0,0,0,0,0,
            0,0,0,0,1,1,0,0,1,1,0,0,0,0,0,0,
            0,0,0,0,0,0,1,1,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,1,1,0,0,0,0,0,0,0,0,
            0,0,0,0,1,1,0,0,1,1,0,0,0,0,0,0,
            0,0,0,1,1,0,0,0,0,1,1,0,0,0,0,0,
            0,0,0,0,1,1,0,0,1,1,0,0,0,0,0,0,
            0,0,0,0,0,0,1,1,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,1,1,0,0,0,0,0,0,0,0,
            0,0,0,0,1,1,0,0,1,1,0,0,0,0,0,0,
            0,0,0,1,1,0,0,0,0,1,1,0,0,0,0,0,
            0,0,0,0,1,1,0,0,1,1,0,0,0,0,0,0,
            0,0,0,0,0,0,1,1,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

    };

    @Test
    public void doDownSample2PerVoxel() throws Exception {
        doDownSample( "2-bytes per voxel/tube", RAW_VOLUME, 8, 8, 8, 2.0, 2 );
    }

    @Test
    public void doDownSampleTube1PerVoxel() throws Exception {
        doDownSample( "1-byte-per-voxel/tube", ONE_BYTE_VOLUME, 16, 8, 8, 2.0, 1 );
    }

    @Test
    public void doDownSampleCircle1PerVoxel() throws Exception {
        doDownSample( "1-byte-per-voxel/circle", CIRCLE_VOLUME, 16, 8, 8, 2.0, 1 );
    }

    public void doDownSample(
            String testName, byte[] volume, int sx, int sy, int sz, double scaleAll, int voxelBytes )
            throws Exception {

        System.out.println("TEST: " + testName );
        double xScale = scaleAll;
        double yScale = scaleAll;
        double zScale = scaleAll;

        DownSampler downSampler = new DownSampler( sx, sy, sz );
        DownSampler.DownsampledTextureData data =
                downSampler.getDownSampledVolume( new VolumeDataBean( volume, sx, sy, sz ), voxelBytes, xScale, yScale, zScale );
        Assert.assertNotSame( "Zero-length volume.", data.getVolume().length(), 0 );
        for (int i = 0; i < data.getVolume().length(); i++ ) {
            if ( i % (data.getSx() * voxelBytes) == 0 ) {
                System.out.println();
            }
            System.out.print( data.getVolume().getValueAt(i)  + ",");
        }
        System.out.println();
        System.out.println( "Length total=" + data.getVolume().length() );
        System.out.println( "Dimensions are " + data.getSx() + " x " + data.getSy() + " x " + data.getSz() );
    }

}
