/*
 * Copyright (c) 2016, Kasra Faghihi, All rights reserved.
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

import org.apache.commons.lang3.Validate;
import org.objectweb.asm.Type;

final class CacheVariables {
    private final Variable intReturnCacheVar;
    private final Variable longReturnCacheVar;
    private final Variable floatReturnCacheVar;
    private final Variable doubleReturnCacheVar;
    private final Variable objectReturnCacheVar;
    private final Variable throwableCacheVar;
    
    CacheVariables(Variable intReturnCacheVar,
                   Variable longReturnCacheVar,
                   Variable floatReturnCacheVar,
                   Variable doubleReturnCacheVar,
                   Variable objectReturnCacheVar,
                   Variable throwableCacheVar) {
        this.intReturnCacheVar = intReturnCacheVar;
        this.longReturnCacheVar = longReturnCacheVar;
        this.floatReturnCacheVar = floatReturnCacheVar;
        this.doubleReturnCacheVar = doubleReturnCacheVar;
        this.objectReturnCacheVar = objectReturnCacheVar;
        
        this.throwableCacheVar = throwableCacheVar;
    }

    public Variable getIntReturnCacheVar() {
        Validate.validState(intReturnCacheVar != null, "Return cache variable of type not assigned");
        return intReturnCacheVar;
    }

    public Variable getLongReturnCacheVar() {
        Validate.validState(longReturnCacheVar != null, "Return cache variable of type not assigned");
        return longReturnCacheVar;
    }

    public Variable getFloatReturnCacheVar() {
        Validate.validState(floatReturnCacheVar != null, "Return cache variable of type not assigned");
        return floatReturnCacheVar;
    }

    public Variable getDoubleReturnCacheVar() {
        Validate.validState(doubleReturnCacheVar != null, "Return cache variable of type not assigned");
        return doubleReturnCacheVar;
    }

    public Variable getObjectReturnCacheVar() {
        Validate.validState(objectReturnCacheVar != null, "Return cache variable of type not assigned");
        return objectReturnCacheVar;
    }

    public Variable getThrowableCacheVar() {
        Validate.validState(throwableCacheVar != null, "Throwable cache variable of type not assigned");
        return throwableCacheVar;
    }
    
    public Variable getReturnCacheVar(Type type) {
        Validate.notNull(type);

        switch (type.getSort()) {
            case Type.BOOLEAN:
            case Type.BYTE:
            case Type.CHAR:
            case Type.SHORT:
            case Type.INT:
                return getIntReturnCacheVar();
            case Type.LONG:
                return getLongReturnCacheVar();
            case Type.FLOAT:
                return getFloatReturnCacheVar();
            case Type.DOUBLE:
                return getDoubleReturnCacheVar();
            case Type.ARRAY:
            case Type.OBJECT:
                return getObjectReturnCacheVar();
            case Type.VOID:
                return null;
            default:
                throw new IllegalArgumentException("Bad type");
        }
    }
}
