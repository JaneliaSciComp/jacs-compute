/*
 * Copyright (c) 2015, Kasra Faghihi, All rights reserved.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3.0 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.
 */
package org.janelia.cont.instrument;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.MethodNode;

/**
 * Tracks extra variables used for instrumentation as well as arguments passed in to a method.
 * @author Kasra Faghihi
 */
final class VariableTable {
    private List<Variable> argVars;
    private int extraOffset;
    private List<Variable> extraVars;
    
    /**
     * Constructs a {@link VariableTable} object.
     * @param classNode class that {@code methodeNode} resides in
     * @param methodNode method this variable table is for
     */
    VariableTable(ClassNode classNode, MethodNode methodNode) {
        this((methodNode.access & Opcodes.ACC_STATIC) != 0,
                Type.getObjectType(classNode.name),
                Type.getType(methodNode.desc),
                methodNode.maxLocals);
        Validate.isTrue(classNode.methods.contains(methodNode)); // sanity check
    }
    
    private VariableTable(boolean isStatic, Type objectType, Type methodType, int maxLocals) {
        Validate.notNull(objectType);
        Validate.notNull(methodType);
        Validate.isTrue(maxLocals >= 0);
        Validate.isTrue(objectType.getSort() == Type.OBJECT);
        Validate.isTrue(methodType.getSort() == Type.METHOD);
        
        extraOffset = maxLocals;
        
        argVars = new ArrayList<>();
        extraVars = new ArrayList<>();
        
        if (!isStatic) {
            argVars.add(0, new Variable(this, objectType, 0, true));
        }
        
        Type[] argTypes = methodType.getArgumentTypes();
        for (int i = 0; i < argTypes.length; i++) {
            int idx = isStatic ? i : i + 1;
            argVars.add(new Variable(this, argTypes[i], idx, true));
        }
    }

    Variable getArgument(int index) {
        Validate.isTrue(index >= 0 && index < argVars.size());
        return argVars.get(index);
    }

    Variable acquireExtra(Class<?> type) {
        Validate.notNull(type);
        
        return acquireExtra(Type.getType(type));
    }
    
    Variable acquireExtra(Type type) {
        Validate.notNull(type);
        Validate.isTrue(type.getSort() != Type.VOID);
        Validate.isTrue(type.getSort() != Type.METHOD);
        
        for (Variable var : extraVars) {
            if (!var.isUsed() && var.getType().equals(type)) {
                // We don't want to return the same object because other objects that may still have the existing Variable will now have
                // them marked as being usable again. Do not want that to be the case. So instead create a new object and return that.
                extraVars.remove(var);
                var = new Variable(this, type, var.getIndex(), true);
                extraVars.add(var);
                return var;
            }
        }
        
        Variable var = new Variable(this, type, extraOffset + extraVars.size(), true);
        extraVars.add(var);
        return var;
    }

    void releaseExtra(Variable variable) {
        variable.setUsed(false);
    }

}
