package org.janelia.jacs2.asyncservice.alignservices;

class Matrix<E> {
    Object[][] elems;

    Matrix(int rows, int cols) {
        elems = new Object[rows][cols];
    }

    @SuppressWarnings("unchecked")
    E getElem(int row, int col) {
        return (E) elems[row][col];
    }

    void setElem(int row, int col, E elem) {
        elems[row][col] = elem;
    }
}
