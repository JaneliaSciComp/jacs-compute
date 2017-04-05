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
import org.janelia.cont.LockState;
import org.objectweb.asm.Type;

final class LockVariables {
    private final Variable lockStateVar;
    private final Variable counterVar;
    private final Variable arrayLenVar;

    LockVariables(
            Variable lockStateVar,
            Variable counterVar,
            Variable arrayLenVar) {
        // vars can be null -- they'll be null if the analyzer determined tehy aren't required
        Validate.isTrue(lockStateVar == null || lockStateVar.getType().equals(Type.getType(LockState.class)));
        Validate.isTrue(counterVar == null || counterVar.getType().equals(Type.INT_TYPE));
        Validate.isTrue(arrayLenVar == null || arrayLenVar.getType().equals(Type.INT_TYPE));

        this.lockStateVar = lockStateVar;
        this.counterVar = counterVar;
        this.arrayLenVar = arrayLenVar;
    }

    public Variable getLockStateVar() {
        return lockStateVar;
    }

    public Variable getCounterVar() {
        return counterVar;
    }

    public Variable getArrayLenVar() {
        return arrayLenVar;
    }
}
