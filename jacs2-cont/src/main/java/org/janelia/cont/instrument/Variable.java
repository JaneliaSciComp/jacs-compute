package org.janelia.cont.instrument;

import org.apache.commons.lang3.Validate;
import org.objectweb.asm.Type;

/**
 * Represents an entry within the local variable table of a method.
 */
final class Variable {
    private VariableTable variableTable;
    private Type type;
    private int index;
    private boolean used;

    Variable(VariableTable variableTable, Type type, int index, boolean used) {
        this.variableTable = variableTable;
        this.type = type;
        this.index = index;
        this.used = used;
    }

    /**
     * Get the type of this local variable table entry.
     * @return type of this entry
     * @throws IllegalArgumentException if this {@link Variable} has been released
     */
    Type getType() {
        Validate.isTrue(used);
        return type;
    }

    /**
     * Get the index of this entry within the local variable table.
     * @return index of this entry
     * @throws IllegalArgumentException if this {@link Variable} has been released
     */
    int getIndex() {
        Validate.isTrue(used);
        return index;
    }

    boolean isUsed() {
        return used;
    }

    void setUsed(boolean used) {
        this.used = used;
    }

    private VariableTable getParent() {
        return variableTable;
    }
}
