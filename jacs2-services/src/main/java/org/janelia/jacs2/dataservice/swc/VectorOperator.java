package org.janelia.jacs2.dataservice.swc;

/**
 * For use of importing and exporting SWC data.  Provides methods for making
 * the exchange between required SWC format and internal Neuron use.
 * 
 * At time of writing: internally, we use voxels, and externally, we
 * use micrometers. If that changes in future, the impl can reflect that,
 * without affecting this interface.  Likewise, double values are exchanged
 * both ways, regardless of whether integers/longs are actually used.
 * 
 * @author fosterl
 */
interface VectorOperator {
    double[] apply(double[] v);
}
