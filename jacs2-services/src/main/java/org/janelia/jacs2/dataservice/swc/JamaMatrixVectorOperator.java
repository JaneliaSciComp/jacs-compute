package org.janelia.jacs2.dataservice.swc;

import Jama.Matrix;
import com.google.common.base.Preconditions;

/**
 * Uses matrices (based on JAMA package), to convert between internal and
 * external SWC coordinate systems.
 *
 * @author fosterl
 */
class JamaMatrixVectorOperator implements VectorOperator {
    private final Matrix transformMatrix;

    JamaMatrixVectorOperator(Matrix transformMatrix) {
        Preconditions.checkArgument(transformMatrix.getColumnDimension() == transformMatrix.getRowDimension());
        this.transformMatrix = transformMatrix;
    }

    @Override
    public double[] apply(double[] v) {
        Matrix inputMatrix = toInputMatrix(v);
        Matrix outputMatrix = transformMatrix.times(inputMatrix);
        return toOutputVector(outputMatrix, v.length);
    }

    private Matrix toInputMatrix(double[] input) {
        int nrows = transformMatrix.getColumnDimension();
        Matrix matrix = new Matrix(nrows, 1);
        for (int i = 0; i < nrows; i++) {
            if (i < input.length)
                matrix.set(i, 0, input[i]);
            else
                matrix.set(i, 0, 1.0);
        }
        return matrix;
    }

    private double[] toOutputVector(Matrix output, int size) {
        double[] result = new double[size];
        for (int i = 0; i < result.length; i++) {
            result[i] = output.get(i, 0);
        }
        return result;
    }

}
