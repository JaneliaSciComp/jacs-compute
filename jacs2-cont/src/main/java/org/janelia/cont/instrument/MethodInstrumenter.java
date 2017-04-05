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

import java.util.List;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.InsnList;
import org.objectweb.asm.tree.InsnNode;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.MethodNode;

final class MethodInstrumenter {

    void instrument(MethodNode methodNode, MethodAttributes attrs) {
        // Add continuation save points
        List<ContinuationPoint> continuationPoints = attrs.getContinuationPoints();
        for (int i = 0; i < continuationPoints.size(); i++) {
            ContinuationPoint cp = continuationPoints.get(i);

            AbstractInsnNode nodeToReplace = cp.getInvokeInstruction();
            InsnList insnsToReplaceWith = CheckpointCodeGenerators.saveState(attrs, i);
            
            methodNode.instructions.insertBefore(nodeToReplace, insnsToReplaceWith);
            methodNode.instructions.remove(nodeToReplace);
        }
        
        // Add synchronization save points
        List<SynchronizationPoint> synchPoints = attrs.getSynchPoints();
        LockVariables lockVars = attrs.getLockVars();
        for (int i = 0; i < synchPoints.size(); i++) {
            SynchronizationPoint sp = synchPoints.get(i);

            InsnNode nodeToReplace = sp.getMonitorInstruction();
            InsnList insnsToReplaceWith;
            switch (nodeToReplace.getOpcode()) {
                case Opcodes.MONITORENTER:
                    insnsToReplaceWith = CheckpointCodeGenerators.enterMonitorAndStore(lockVars);
                    break;
                case Opcodes.MONITOREXIT:
                    insnsToReplaceWith = CheckpointCodeGenerators.exitMonitorAndDelete(lockVars);
                    break;
                default:
                    throw new IllegalStateException(); //should never happen
            }
            
            methodNode.instructions.insertBefore(nodeToReplace, insnsToReplaceWith);
            methodNode.instructions.remove(nodeToReplace);
        }
    }
}
