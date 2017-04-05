package org.janelia.cont.instrument;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.janelia.cont.Continuation;
import org.janelia.cont.LockState;
import org.janelia.cont.MethodState;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.InsnList;
import org.objectweb.asm.tree.InsnNode;
import org.objectweb.asm.tree.LabelNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.VarInsnNode;
import org.objectweb.asm.tree.analysis.BasicValue;
import org.objectweb.asm.tree.analysis.Frame;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

class CheckpointCodeGenerators {

    private static final Method LOCKSTATE_ENTER_METHOD
            = MethodUtils.getAccessibleMethod(LockState.class, "enter", Object.class);
    private static final Method LOCKSTATE_EXIT_METHOD
            = MethodUtils.getAccessibleMethod(LockState.class, "exit", Object.class);
    private static final Method LOCKSTATE_TOARRAY_METHOD
            = MethodUtils.getAccessibleMethod(LockState.class, "toArray");

    private static final Constructor<MethodState> METHODSTATE_INIT_METHOD
            = ConstructorUtils.getAccessibleConstructor(MethodState.class, Integer.TYPE, Object[].class, LockState.class);

    private static final Method CONTINUATION_SAVECHECKPOINT_METHOD
            = MethodUtils.getAccessibleMethod(Continuation.class, "saveCheckpoint", MethodState.class);

    static InsnList saveState(MethodAttributes attrs, int idx) {

        InsnList saveInsnList;
        ContinuationPoint continuationPoint = attrs.getContinuationPoint(idx);

        if (continuationPoint instanceof CheckpointContinuationPoint) {
            CheckpointContinuationPoint ckpCp = (CheckpointContinuationPoint) continuationPoint;
            saveInsnList = saveStateOnCheckpoint(attrs, ckpCp, idx);
        } else {
            throw new IllegalArgumentException(); // should never happen
        }

        return saveInsnList;
    }

    private static InsnList saveStateOnCheckpoint(MethodAttributes attrs, CheckpointContinuationPoint cp, int pos) {
        Integer lineNumber = cp.getLineNumber();

//        Variable contArg = attrs.getCoreVariables().getContinuationArgVar();
        VariablesStorage savedLocalsVars = attrs.getLocalsStorage();
        VariablesStorage savedStackVars = attrs.getStackStorage();
        Variable storageContainerVar = attrs.getStorageContainerVars().getContainerVar();

        LockVariables lockVars = attrs.getLockVars();
        Variable lockStateVar = lockVars.getLockStateVar();

        Frame<BasicValue> frame = cp.getFrame();
        LabelNode continueExecLabelNode = cp.getContinueExecutionLabel();

        //  Object[] stack = saveOperandStack();
        //  Object[] locals = saveLocals();
        //  continuation.saveCheckpoint(new MethodState(<number>, stack, locals, lockState);
        //
        //  restorePoint_<number>_continue: // at this label: empty exec stack / uninit exec var table
        VariableStorageContainer contArg = attrs.getContStorage();
        return BasicCodeGenerators.merge(
                BasicCodeGenerators.mergeIf(lineNumber != null, () -> new Object[]{
                        BasicCodeGenerators.generateLineNumber(lineNumber)
                }),
                OperandStackStateGenerators.saveOperandStack(savedStackVars, frame),
                LocalsStateGenerators.saveLocals(savedLocalsVars, frame),
                PackStateCodeGenerators.packStorageArrays(frame, storageContainerVar, savedLocalsVars, savedStackVars),
                BasicCodeGenerators.call(CONTINUATION_SAVECHECKPOINT_METHOD,
                        BasicCodeGenerators.loadVar(contArg.getContainerVar()),
                        BasicCodeGenerators.construct(METHODSTATE_INIT_METHOD,
                                BasicCodeGenerators.loadIntConst(pos),
                                BasicCodeGenerators.loadVar(storageContainerVar),
                                // load lockstate for last arg if method actually has monitorenter/exit in it
                                // (var != null if this were the case), otherwise load null for that arg
                                BasicCodeGenerators.mergeIf(lockStateVar != null, () -> new Object[]{
                                        BasicCodeGenerators.loadVar(lockStateVar)
                                }).mergeIf(lockStateVar == null, () -> new Object[]{
                                        BasicCodeGenerators.loadNull()
                                }).generate()
                        )
                ),
                BasicCodeGenerators.addLabel(continueExecLabelNode)
        );
    }

    static InsnList enterMonitorAndStore(LockVariables lockVars) {
        Variable lockStateVar = lockVars.getLockStateVar();

        Type clsType = Type.getType(LOCKSTATE_ENTER_METHOD.getDeclaringClass());
        Type methodType = Type.getType(LOCKSTATE_ENTER_METHOD);
        String clsInternalName = clsType.getInternalName();
        String methodDesc = methodType.getDescriptor();
        String methodName = LOCKSTATE_ENTER_METHOD.getName();

        // NOTE: This adds to the lock state AFTER locking.
        return BasicCodeGenerators.merge(
                // [obj]
                new InsnNode(Opcodes.DUP),                               // [obj, obj]
                new InsnNode(Opcodes.MONITORENTER),                      // [obj]
                new VarInsnNode(Opcodes.ALOAD, lockStateVar.getIndex()), // [obj, lockState]
                new InsnNode(Opcodes.SWAP),                              // [lockState, obj]
                new MethodInsnNode(Opcodes.INVOKEVIRTUAL,                // []
                        clsInternalName,
                        methodName,
                        methodDesc,
                        false)
        );
    }

    static InsnList exitMonitorAndDelete(LockVariables lockVars) {
        Variable lockStateVar = lockVars.getLockStateVar();

        Type clsType = Type.getType(LOCKSTATE_EXIT_METHOD.getDeclaringClass());
        Type methodType = Type.getType(LOCKSTATE_EXIT_METHOD);
        String clsInternalName = clsType.getInternalName();
        String methodDesc = methodType.getDescriptor();
        String methodName = LOCKSTATE_EXIT_METHOD.getName();

        // NOTE: This removes the lock AFTER unlocking.
        return BasicCodeGenerators.merge(
                // [obj]
                new InsnNode(Opcodes.DUP),                               // [obj, obj]
                new InsnNode(Opcodes.MONITOREXIT),                       // [obj]
                new VarInsnNode(Opcodes.ALOAD, lockStateVar.getIndex()), // [obj, lockState]
                new InsnNode(Opcodes.SWAP),                              // [lockState, obj]
                new MethodInsnNode(Opcodes.INVOKEVIRTUAL,                // []
                        clsInternalName,
                        methodName,
                        methodDesc,
                        false)
        );
    }

    static InsnList enterStoredMonitors(LockVariables lockVars) {

        Variable lockStateVar = lockVars.getLockStateVar();
        Variable counterVar = lockVars.getCounterVar();
        Variable arrayLenVar = lockVars.getArrayLenVar();

        return BasicCodeGenerators.forEach(counterVar, arrayLenVar,
                BasicCodeGenerators.merge(
                        BasicCodeGenerators.call(LOCKSTATE_TOARRAY_METHOD, BasicCodeGenerators.loadVar(lockStateVar))
                ),
                BasicCodeGenerators.merge(
                        new InsnNode(Opcodes.MONITORENTER)
                )
        );
    }

    static InsnList exitStoredMonitors(LockVariables lockVars) {

        Variable lockStateVar = lockVars.getLockStateVar();
        Variable counterVar = lockVars.getCounterVar();
        Variable arrayLenVar = lockVars.getArrayLenVar();

        return BasicCodeGenerators.forEach(counterVar, arrayLenVar,
                BasicCodeGenerators.merge(
                        BasicCodeGenerators.call(LOCKSTATE_TOARRAY_METHOD, BasicCodeGenerators.loadVar(lockStateVar))
                ),
                BasicCodeGenerators.merge(
                        new InsnNode(Opcodes.MONITOREXIT)
                )
        );
    }

    private static InsnList returnDummyVal(Type returnType) {
        InsnList ret = new InsnList();

        switch (returnType.getSort()) {
            case Type.VOID:
                ret.add(new InsnNode(Opcodes.RETURN));
                break;
            case Type.BOOLEAN:
            case Type.BYTE:
            case Type.SHORT:
            case Type.CHAR:
            case Type.INT:
                ret.add(new InsnNode(Opcodes.ICONST_0));
                ret.add(new InsnNode(Opcodes.IRETURN));
                break;
            case Type.LONG:
                ret.add(new InsnNode(Opcodes.LCONST_0));
                ret.add(new InsnNode(Opcodes.LRETURN));
                break;
            case Type.FLOAT:
                ret.add(new InsnNode(Opcodes.FCONST_0));
                ret.add(new InsnNode(Opcodes.FRETURN));
                break;
            case Type.DOUBLE:
                ret.add(new InsnNode(Opcodes.DCONST_0));
                ret.add(new InsnNode(Opcodes.DRETURN));
                break;
            case Type.OBJECT:
            case Type.ARRAY:
                ret.add(new InsnNode(Opcodes.ACONST_NULL));
                ret.add(new InsnNode(Opcodes.ARETURN));
                break;
            default:
                throw new IllegalStateException();
        }
        return ret;
    }

}
