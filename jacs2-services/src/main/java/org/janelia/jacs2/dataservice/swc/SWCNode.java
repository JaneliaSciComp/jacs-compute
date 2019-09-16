package org.janelia.jacs2.dataservice.swc;

/**
 * used by the SWCData class to hold each point in the neuron
 * <p>
 * see http://research.mssm.edu/cnic/swc.html; node holds info
 * from one line in SWC file
 */
public class SWCNode {

    private final int index;
    private final int segmentType;
    private final double x, y, z;
    private final double radius;
    private final int parentIndex;

    /**
     * @param index       = index of node
     * @param segmentType = segment type; see types in code comments
     * @param x,          y, z = location
     * @param radius      = radius at node
     * @param parentIndex = index of parent node (-1 = no parent)
     */
    SWCNode(int index, int segmentType, double x, double y, double z,
            double radius, int parentIndex) {

        this.index = index;
        this.segmentType = segmentType;
        this.x = x;
        this.y = y;
        this.z = z;
        this.radius = radius;
        this.parentIndex = parentIndex;

    }

    /**
     * simple validity checks; not returning a reason at this point
     */
    boolean isValid() {
        // couple simple validity checks
        if (radius <= 0.0) {
            return false;
        }
        return true;
    }

    /**
     * returns a string (no newline) that is suitable for writing the node
     * into an SWC file
     * <p>
     * note: apparently some SWC readers are picky about the separator;
     * single-space is apparently the best choice for compatibility
     * (we will import any number of whitespace as separator)
     */
    String toSWCline() {
        return String.format("%d %d %f %f %f %f %d",
                index,
                segmentType,
                x, y, z,
                radius,
                parentIndex);
    }

    public int getIndex() {
        return index;
    }

    public int getSegmentType() {
        return segmentType;
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    public double getZ() {
        return z;
    }

    public double getRadius() {
        return radius;
    }

    public int getParentIndex() {
        return parentIndex;
    }

}
