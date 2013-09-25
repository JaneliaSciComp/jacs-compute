package org.janelia.it.FlyWorkstation.tracing;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import org.janelia.it.FlyWorkstation.gui.slice_viewer.Subvolume;
import org.janelia.it.FlyWorkstation.octree.ZoomLevel;
import org.janelia.it.FlyWorkstation.octree.ZoomedVoxelIndex;
import org.janelia.it.FlyWorkstation.raster.VoxelIndex;

import com.google.common.collect.Lists;

/**
 * Maybe implement my own version of AStar.
 * http://en.wikipedia.org/wiki/A*_search_algorithm
 * 
 * @author brunsc
 *
 * TODO - store log(probability), not probability
 */
public class AStar {
    enum DistanceMetric {
        EUCLIDEAN,
        MANHATTAN,
    }
    private DistanceMetric distanceMetric = DistanceMetric.EUCLIDEAN;
    
    // How many neighbors to examine for each voxel?
    // TODO - not implemented yet
    enum NeighborClass {
        SIX_CONNECTED,
        // EIGHTEEN_CONNECTED, // I don't have all day here...
        TWENTYSIX_CONNECTED,
    }
    private NeighborClass neighborClass = NeighborClass.TWENTYSIX_CONNECTED;
    
    private boolean debug = false;
    // Cached values
    // stepCostLowerBound has a dramatic effect on performance 9-25-2013
    // Numbers larger than <some small amount> take more time and cause more nodes to be explored.
    // Non-zero values prevent meandering path.
    private final double stepCostLowerBound = 1e-60;
    
    private double minStepCost = Double.NaN; // will be set from volume statistics
    private Subvolume volume;
    private Map<Integer, Double> pathCostForIntensity = new HashMap<Integer, Double>();
    private double meanIntensity = Double.NaN;
    private double stdDevIntensity = Double.NaN;
    // In case of anisotropic voxel size
    private double voxelSizeX = 1.0;
    private double voxelSizeY = 1.0;
    private double voxelSizeZ = 1.0;
    
    public AStar(Subvolume volume) {
        this.volume = volume;
        computeIntensityStats();
    }
    
    public List<ZoomedVoxelIndex> trace(
            ZoomedVoxelIndex start0, 
            ZoomedVoxelIndex goal0) 
    {
        VoxelIndex start = new VoxelIndex(
                start0.getX() - volume.getOrigin().getX(), 
                start0.getY() - volume.getOrigin().getY(), 
                start0.getZ() - volume.getOrigin().getZ());
        VoxelIndex goal = new VoxelIndex(
                goal0.getX() - volume.getOrigin().getX(), 
                goal0.getY() - volume.getOrigin().getY(), 
                goal0.getZ() - volume.getOrigin().getZ());
        // The set of nodes already evaluated
        Set<VoxelIndex> closedSet = new HashSet<VoxelIndex>();
        // The set of tentative nodes to be evaluated, initially containing the start node.
        Set<VoxelIndex> openSet = new HashSet<VoxelIndex>();
        openSet.add(new VoxelIndex(start.getX(), start.getY(), start.getZ()));
        // The map of navigated nodes
        Map<VoxelIndex, VoxelIndex> cameFrom = new HashMap<VoxelIndex, VoxelIndex>();
        // Cost from start along best known path
        Map<VoxelIndex, Double> gScore = new HashMap<VoxelIndex, Double>();
        Map<VoxelIndex, Double> fScore = new HashMap<VoxelIndex, Double>();
        gScore.put(start, 0.0);
        fScore.put(start, gScore.get(start) + heuristicCostEstimate(start, goal));
        
        long checkedVoxelCount = 0;
        while (openSet.size() > 0) {
            // Get node with lowest fScore in openSet
            // TODO - perhaps a sorted list could be maintained, to avoid 
            // exhaustive search each time.
            VoxelIndex current = null;
            for (VoxelIndex n : openSet) {
                if (current == null) {
                    current = n;
                    continue;
                }
                if (fScore.get(n) < fScore.get(current))
                    current = n;
            }
            if (current.equals(goal))
                return reconstructPath(cameFrom, start, goal, start0.getZoomLevel());
            // Remove current from openSet
            openSet.remove(current);
            closedSet.add(current);
            checkedVoxelCount += 1;
            if (debug && checkedVoxelCount % 10000 == 0)
                System.out.println("Examined "+checkedVoxelCount+" voxels");
            for (VoxelIndex neighbor : getNeighbors(current)) 
            {
                double tentativeGScore = gScore.get(current)
                        + distanceBetween(current, neighbor);
                if ( closedSet.contains(neighbor)
                       && (tentativeGScore >= gScore.get(neighbor) ) ) {
                    continue;
                }
                if ( (! openSet.contains(neighbor)) 
                       || (tentativeGScore < gScore.get(neighbor)) )
                {
                    cameFrom.put(neighbor, current);
                    gScore.put(neighbor, tentativeGScore);
                    fScore.put(neighbor, gScore.get(neighbor) + heuristicCostEstimate(neighbor, goal));
                    openSet.add(neighbor);
                }
            }
        }
        return null;
    }
    
    private double distanceBetween(VoxelIndex current, VoxelIndex neighbor) {
        // Set distance to cost of second node.
        int intensity = volume.getIntensityLocal(neighbor, 0);
        double pathScore = getPathStepCostForIntensity(intensity);
        // Use Manhattan distance, and prohibit diagonal moves (for performance)
        double dx = (current.getX() - neighbor.getX()) * voxelSizeX;
        double dy = (current.getY() - neighbor.getY()) * voxelSizeY;
        double dz = (current.getZ() - neighbor.getZ()) * voxelSizeZ;
        double distance = 0;
        if (distanceMetric == DistanceMetric.MANHATTAN) {
            distance += Math.abs(dx);
            distance += Math.abs(dy);
            distance += Math.abs(dz);
        } else if (distanceMetric == DistanceMetric.EUCLIDEAN) {
            distance = Math.sqrt(dx*dx + dy*dy + dz*dz);
        }
        return pathScore * distance;
    }
    
    private List<VoxelIndex> getNeighbors(VoxelIndex center) {
        // For performance, don't step to diagonals
        // Thus, up to six neighbors in 3D
        List<VoxelIndex> result = new Vector<VoxelIndex>();
        //
        if (neighborClass == NeighborClass.SIX_CONNECTED) {
            if (center.getX() > 0)
                result.add(new VoxelIndex(center.getX()-1, center.getY(), center.getZ()));
            if (center.getY() > 0)
                result.add(new VoxelIndex(center.getX(), center.getY()-1, center.getZ()));
            if (center.getZ() > 0)
                result.add(new VoxelIndex(center.getX(), center.getY(), center.getZ()-1));
            //
            if (center.getX() < volume.getExtent().getX() - 1)
                result.add(new VoxelIndex(center.getX()+1, center.getY(), center.getZ()));
            if (center.getX() < volume.getExtent().getY() - 1)
                result.add(new VoxelIndex(center.getX(), center.getY()+1, center.getZ()));
            if (center.getX() < volume.getExtent().getZ() - 1)
                result.add(new VoxelIndex(center.getX(), center.getY(), center.getZ()+1));
        }
        else if (neighborClass == NeighborClass.TWENTYSIX_CONNECTED) {
            for (int dx = -1; dx <= 1; ++dx) {
                int x = center.getX() + dx;
                if (x < 0) continue;
                if (x >= volume.getExtent().getX() - 1) continue;
                for (int dy = -1; dy <= 1; ++dy) {
                    int y = center.getY() + dy;
                    if (y < 0) continue;
                    if (y >= volume.getExtent().getY() - 1) continue;
                    for (int dz = -1; dz <= 1; ++dz) {
                        if ((dx == 0) && (dy == 0) && (dz == 0)) 
                            continue; // self is not a neighbor
                        int z = center.getZ() + dz;
                        if (z < 0) continue;
                        if (z >= volume.getExtent().getZ() - 1) continue;
                        result.add(new VoxelIndex(x, y, z));
                    }
                }
            }
        }
        //
        return result;
    }
    
    private List<ZoomedVoxelIndex> reconstructPath(
            Map<VoxelIndex, VoxelIndex> cameFrom, 
            VoxelIndex start,
            VoxelIndex goal,
            ZoomLevel zoomLevel) 
    {
        List<ZoomedVoxelIndex> result = new Vector<ZoomedVoxelIndex>();
        VoxelIndex p = goal;
        while (! p.equals(start)) {
            result.add(new ZoomedVoxelIndex(zoomLevel, 
                    p.getX() + volume.getOrigin().getX(), 
                    p.getY() + volume.getOrigin().getY(), 
                    p.getZ() + volume.getOrigin().getZ()));
            p = cameFrom.get(p);
        }
        result.add(new ZoomedVoxelIndex(zoomLevel, 
                start.getX() + volume.getOrigin().getX(), 
                start.getY() + volume.getOrigin().getY(), 
                start.getZ() + volume.getOrigin().getZ()));
        return Lists.reverse(result);
    }

    // Compute mean, standard deviation, and minimum path score
    void computeIntensityStats() {
        double sumIntensity = 0;
        long intensityCount = 0;
        int maxIntensity = Integer.MIN_VALUE;
        ByteBuffer intensityBytes = volume.getByteBuffer();
        // Mean and min path
        if (volume.getBytesPerIntensity() == 2) {
            // two bytes per value ushort
            ShortBuffer shorts = intensityBytes.asShortBuffer();
            shorts.rewind();
            while (shorts.hasRemaining()) {
                int intensity = shorts.get() & 0xffff;
                maxIntensity = Math.max(intensity, maxIntensity);
                sumIntensity += intensity;
                intensityCount += 1;
            }
        }
        else { // one byte per value ubyte
            intensityBytes.rewind();
            while (intensityBytes.hasRemaining()) {
                int intensity = intensityBytes.get() & 0xff;
                maxIntensity = Math.max(intensity, maxIntensity);
                sumIntensity += intensity;
                intensityCount += 1;
            }
        }
        meanIntensity = 0.0;
        if (intensityCount > 0)
            meanIntensity = sumIntensity / (double)intensityCount;
        // Standard deviation
        double delta = 0;
        if (volume.getBytesPerIntensity() == 2) {
            // two bytes per value ushort
            ShortBuffer shorts = intensityBytes.asShortBuffer();
            shorts.rewind();
            while (shorts.hasRemaining()) {
                int intensity = shorts.get() & 0xffff;
                double di = meanIntensity - intensity;
                delta += di * di;
            }
        }
        else { // one byte per value ubyte
            intensityBytes.rewind();
            while (intensityBytes.hasRemaining()) {
                int intensity = intensityBytes.get() & 0xff;
                double di = meanIntensity - intensity;
                delta += di * di;
            }
        }
        stdDevIntensity = 1.0;
        if (intensityCount > 0) 
            stdDevIntensity = Math.sqrt(delta/(double)intensityCount);
        // minStepCost must be computed AFTER mean/stddev
        minStepCost = getPathStepCostForIntensity(maxIntensity) 
                + stepCostLowerBound
                ;
    }
    
    // fractional error in math formula less than 1.2 * 10 ^ -7.
    // although subject to catastrophic cancellation when z in very close to 0
    // from Chebyshev fitting formula for erf(z) from Numerical Recipes, 6.2
    // CMB - return 1-erf for better numerical precision at high Z
    public static double oneMinusErf(double z) {
        double t = 1.0 / (1.0 + 0.5 * Math.abs(z));

        // use Horner's method
        double result = t * Math.exp( -z*z   -   1.26551223 +
                                            t * ( 1.00002368 +
                                            t * ( 0.37409196 + 
                                            t * ( 0.09678418 + 
                                            t * (-0.18628806 + 
                                            t * ( 0.27886807 + 
                                            t * (-1.13520398 + 
                                            t * ( 1.48851587 + 
                                            t * (-0.82215223 + 
                                            t * ( 0.17087277))))))))));
        if (z < 0) 
            result = 2.0 - result;
        return  result;
    }

    // Let path step cost be the probability that this intensity could 
    // occur by chance, given the intensity statistics.
    private double getPathStepCostForIntensity(int intensity) {
        double result;
        if (pathCostForIntensity.containsKey(intensity))
            result = pathCostForIntensity.get(intensity);
        else {
            double zScore = (intensity - meanIntensity) / stdDevIntensity;
            // Reduce Z-score by a factor, so we can numerically distinguish more very bright values
            final double zFudge = 0.80;
            result = oneMinusErf(zFudge*zScore);
            // Store computed value for future use
            pathCostForIntensity.put(intensity, result);
        }
        return result;
    }

    // Must not overestimate actual cost of path to goal
    double heuristicCostEstimate(VoxelIndex v1, VoxelIndex v2) {
        double dx = (v1.getX() - v2.getX()) * voxelSizeX;
        double dy = (v1.getY() - v2.getY()) * voxelSizeY;
        double dz = (v1.getZ() - v2.getZ()) * voxelSizeZ;
        double distance = 0;
        if (distanceMetric == DistanceMetric.MANHATTAN) {
            // Use Manhattan distance, and prohibit diagonal moves (for performance)
            distance += Math.abs(dx);
            distance += Math.abs(dy);
            distance += Math.abs(dz);
        } else if (distanceMetric == DistanceMetric.EUCLIDEAN) {
            distance = Math.sqrt(dx*dx + dy*dy + dz*dz);
        }
        return distance * minStepCost;
    }
    
}
