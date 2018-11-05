package org.janelia.jacs2.dataservice.swc;

/**
 * used by the SWCData class to hold each point in the neuron
 * <p>
 * see http://research.mssm.edu/cnic/swc.html; node holds info
 * from one line in SWC file
 */
public class SWCNode {

    public enum SegmentType {
        undefined(0),
        soma(1),
        axon(2),
        dendrite(3),
        apical_dendrite(4),
        fork_point(5),
        end_point(6),
        custom(7);

        private int decodeNum;

        SegmentType(int decodeNum) {
            this.decodeNum = decodeNum;
        }

        public int decode() {
            return decodeNum;
        }

        public static SegmentType fromNumValue(int num) {
            for (SegmentType st : values()) {
                if (st.decodeNum == num) {
                    return st;
                }
            }
            throw new IllegalArgumentException("Invalid segnebt type value: " + num);
        }

        @Override
        public String toString() {
            return this.name().replaceAll("_", " ");
        }
    }

    private final int index;
    private final SegmentType segmentType;
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
    SWCNode(int index, SegmentType segmentType, double x, double y, double z,
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
                segmentType.decode(),
                x, y, z,
                radius,
                parentIndex);
    }

    public int getIndex() {
        return index;
    }

    public SegmentType getSegmentType() {
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
