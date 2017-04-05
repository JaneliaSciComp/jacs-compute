package org.janelia.cont.instrument;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.janelia.cont.Continuation;
import org.janelia.cont.LockState;
import org.janelia.cont.MethodState;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.InsnNode;
import org.objectweb.asm.tree.LineNumberNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.analysis.Analyzer;
import org.objectweb.asm.tree.analysis.AnalyzerException;
import org.objectweb.asm.tree.analysis.BasicValue;
import org.objectweb.asm.tree.analysis.Frame;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class MethodAnalyzer {

    private static final Type CONTINUATION_CLASS_TYPE = Type.getType(Continuation.class);
    private static final Method CONTINUATION_CHECKPOINT_METHOD = MethodUtils.getAccessibleMethod(Continuation.class, "checkpoint");

    private static final class TypeTracker {
        private boolean intFound = false;
        private boolean longFound = false;
        private boolean floatFound = false;
        private boolean doubleFound = false;
        private boolean objectFound = false;
        private boolean insideTryCatchBlock = false;

        void trackType(Type type) {
            switch (type.getSort()) {
                case Type.BOOLEAN:
                case Type.BYTE:
                case Type.CHAR:
                case Type.SHORT:
                case Type.INT:
                    intFound = true;
                    break;
                case Type.LONG:
                    longFound = true;
                    break;
                case Type.FLOAT:
                    floatFound = true;
                    break;
                case Type.DOUBLE:
                    doubleFound = true;
                    break;
                case Type.OBJECT:
                case Type.ARRAY:
                    objectFound = true;
                    break;
                case Type.VOID:
                case Type.METHOD:
                default:
                    throw new IllegalArgumentException(); // this should never happen
            }
        }

        boolean isInsideTryCatchBlock() {
            return insideTryCatchBlock;
        }

        void setInsideTryCatchBlock(boolean insideTryCatchBlock) {
            this.insideTryCatchBlock = insideTryCatchBlock;
        }
    }

    private final ClassInformationRepository classRepo;

    MethodAnalyzer(ClassInformationRepository classRepo) {
        this.classRepo = classRepo;
    }

    Optional<MethodAttributes> analyze(ClassNode classNode, MethodNode methodNode) {

        List<AbstractInsnNode> checkpointInvocationInsnNodes
                = AsmUtils.findInvocationsOf(methodNode.instructions, CONTINUATION_CHECKPOINT_METHOD);
        List<AbstractInsnNode> contInvocationInsnNodes
                = AsmUtils.findInvocationsWithParameter(methodNode.instructions, CONTINUATION_CLASS_TYPE);

        if (checkpointInvocationInsnNodes.isEmpty() && contInvocationInsnNodes.isEmpty()) {
            // nothing to do
            return Optional.empty();
        }

        // Find MONITORENTER/MONITOREXIT instructions
        List<AbstractInsnNode> monitorInsnNodes = AsmUtils.findOpcodes(methodNode.instructions, Opcodes.MONITORENTER, Opcodes.MONITOREXIT);

        // Compute frames for each instruction in the method
        Frame<BasicValue>[] frames;
        try {
            frames = new Analyzer<>(new SimpleVerifier(classRepo)).analyze(classNode.name, methodNode);
        } catch (AnalyzerException ae) {
            throw new IllegalArgumentException("Analyzer failed to analyze method", ae);
        }

        List<ContinuationPoint> continuationPoints = new LinkedList<>();

        // scan checkpoint invocations
        for (AbstractInsnNode checkpointInvocationInsNode : checkpointInvocationInsnNodes) {
            int instructionIndex = methodNode.instructions.indexOf(checkpointInvocationInsNode);
            Frame<BasicValue> frame = frames[instructionIndex];
            Optional<LineNumberNode> lineNumberNode = AsmUtils.findLineNumberForInstruction(methodNode.instructions, checkpointInvocationInsNode);
            Integer lineNumber = lineNumberNode
                    .map(lnn -> lnn.line)
                    .orElse(null);

            CheckpointContinuationPoint checkpoint = new CheckpointContinuationPoint(
                    lineNumber, (MethodInsnNode) checkpointInvocationInsNode, frame);
            continuationPoints.add(checkpoint);
        }

        // scan method invocations that have a continuation parameter
        for (AbstractInsnNode contInvocationInsnNode : contInvocationInsnNodes) {
            int instructionIndex = methodNode.instructions.indexOf(contInvocationInsnNode);
            boolean withinTryCatch = AsmUtils.findTryCatchBlockNodesEncompassingInstruction(
                    methodNode.instructions,
                    methodNode.tryCatchBlocks,
                    contInvocationInsnNode).size() > 0;
            Frame<BasicValue> frame = frames[instructionIndex];

            Optional<LineNumberNode> lineNumberNode = AsmUtils.findLineNumberForInstruction(methodNode.instructions, contInvocationInsnNode);
            Integer lineNumber = lineNumberNode
                    .map(lnn -> lnn.line)
                    .orElse(null);

            ContinuationPoint continuationPoint;
            if (withinTryCatch) {
                continuationPoint = new TryCatchInvokeContinuationPoint(lineNumber, (MethodInsnNode) contInvocationInsnNode, frame);
            } else {
                continuationPoint = new NormalInvokeContinuationPoint(lineNumber, (MethodInsnNode) contInvocationInsnNode, frame);
            }
            continuationPoints.add(continuationPoint);
        }

        // collect the synchronization points
        List<SynchronizationPoint> synchPoints = new LinkedList<>();
        for (AbstractInsnNode monitorInsnNode : monitorInsnNodes) {
            int instructionIndex = methodNode.instructions.indexOf(monitorInsnNode);
            Frame<BasicValue> frame = frames[instructionIndex];

            SynchronizationPoint synchPoint = new SynchronizationPoint((InsnNode) monitorInsnNode, frame);
            synchPoints.add(synchPoint);
        }

        TypeTracker invocationReturnTypes = trackReturns(methodNode, contInvocationInsnNodes);
        TypeTracker localsTypes = trackLocals(methodNode, CollectionUtils.union(contInvocationInsnNodes, checkpointInvocationInsnNodes), frames);
        TypeTracker operandStackTypes = trackOperandStack(methodNode, CollectionUtils.union(contInvocationInsnNodes, checkpointInvocationInsnNodes), frames);

        VariableTable varTable = new VariableTable(classNode, methodNode);

        VariablesStorage localsStorageVars = allocateStorageVariableSlots(varTable, localsTypes);
        VariablesStorage stackStorageVars = allocateStorageVariableSlots(varTable, operandStackTypes);
        VariableStorageContainer continuationStorage = new VariableStorageContainer(varTable.acquireExtra((Continuation.class)));
        VariableStorageContainer methodStateContainer = new VariableStorageContainer(varTable.acquireExtra(MethodState.class));
        VariableStorageContainer dataContainer = new VariableStorageContainer(varTable.acquireExtra(Object[].class));
        CacheVariables cacheVars = allocateCacheVariableSlots(varTable, invocationReturnTypes);
        LockVariables lockVars = allocateLockVariableSlots(varTable, !synchPoints.isEmpty());

        MethodAttributes methodAttributes = new MethodAttributes(continuationPoints,
                synchPoints,
                continuationStorage,
                localsStorageVars,
                stackStorageVars,
                methodStateContainer,
                dataContainer,
                cacheVars,
                lockVars);

        return Optional.of(methodAttributes);
    }

    private TypeTracker trackReturns(MethodNode methodNode, List<AbstractInsnNode> invocationInsnNodes) {
        TypeTracker invocationReturnTypes = new TypeTracker();
        for (AbstractInsnNode invokeInsnNode : invocationInsnNodes) {
            if (AsmUtils.findTryCatchBlockNodesEncompassingInstruction(
                    methodNode.instructions,
                    methodNode.tryCatchBlocks,
                    invokeInsnNode).size() > 0) {
                invocationReturnTypes.setInsideTryCatchBlock(true);
            }
            Type returnType = AsmUtils.getReturnTypeOfInvocation(invokeInsnNode);
            if (returnType.getSort() != Type.VOID) {
                invocationReturnTypes.trackType(returnType);
            }
        }
        return invocationReturnTypes;
    }

    private TypeTracker trackLocals(MethodNode methodNode, Collection<AbstractInsnNode> invocationInsnNodes, Frame<BasicValue>[] frames) {
        TypeTracker localsTypes = new TypeTracker();
        for (AbstractInsnNode invokeInsnNode : invocationInsnNodes) {
            int instructionIndex = methodNode.instructions.indexOf(invokeInsnNode);
            Frame<BasicValue> frame = frames[instructionIndex];

            for (int i = 0; i < frame.getLocals(); i++) {
                BasicValue basicValue = frame.getLocal(i);
                Type type = basicValue.getType();

                // If type == null, basicValue is pointing to uninitialized var -- basicValue.toString() will return '.'. This means that
                // this slot contains nothing to save. We don't store anything in this case so skip this slot if we encounter it.
                //
                // If type is 'Lnull;', this means that the slot has been assigned null and that "there has been no merge yet that would
                // 'raise' the type toward some class or interface type" (from ASM mailing list). We know this slot will always contain null
                // at this point in the code so we can avoid saving it. When we load it back up, we can simply push a null in to that slot,
                // thereby keeping the same 'Lnull;' type.
                if (type == null || "Lnull;".equals(type.getDescriptor())) {
                    continue;
                }

                localsTypes.trackType(type);
            }
        }
        return localsTypes;
    }

    private TypeTracker trackOperandStack(MethodNode methodNode, Collection<AbstractInsnNode> invocationInsnNodes, Frame<BasicValue>[] frames) {
        TypeTracker operandStackTypes = new TypeTracker();
        for (AbstractInsnNode invokeInsnNode : invocationInsnNodes) {
            int instructionIndex = methodNode.instructions.indexOf(invokeInsnNode);
            Frame<BasicValue> frame = frames[instructionIndex];

            for (int i = 0; i < frame.getStackSize(); i++) {
                BasicValue basicValue = frame.getStack(i);
                Type type = basicValue.getType();

                // If type is 'Lnull;', this means that the slot has been assigned null and that "there has been no merge yet that would
                // 'raise' the type toward some class or interface type" (from ASM mailing list). We know this slot will always contain null
                // at this point in the code so we can avoid saving it. When we load it back up, we can simply push a null in to that slot,
                // thereby keeping the same 'Lnull;' type.
                if ("Lnull;".equals(type.getDescriptor())) {
                    continue;
                }

                operandStackTypes.trackType(type);
            }
        }
        return operandStackTypes;
    }

    private CacheVariables allocateCacheVariableSlots(VariableTable varTable, TypeTracker invocationReturnTypes) {
        Variable intReturnCacheVar = null;
        Variable longReturnCacheVar = null;
        Variable floatReturnCacheVar = null;
        Variable doubleReturnCacheVar = null;
        Variable objectReturnCacheVar = null;
        Variable throwableCacheVar = null;
        if (invocationReturnTypes.intFound) {
            intReturnCacheVar = varTable.acquireExtra(Integer.TYPE);
        }
        if (invocationReturnTypes.longFound) {
            longReturnCacheVar = varTable.acquireExtra(Long.TYPE);
        }
        if (invocationReturnTypes.floatFound) {
            floatReturnCacheVar = varTable.acquireExtra(Float.TYPE);
        }
        if (invocationReturnTypes.doubleFound) {
            doubleReturnCacheVar = varTable.acquireExtra(Double.TYPE);
        }
        if (invocationReturnTypes.objectFound) {
            objectReturnCacheVar = varTable.acquireExtra(Object.class);
        }
        if (invocationReturnTypes.isInsideTryCatchBlock()) {
            throwableCacheVar = varTable.acquireExtra(Throwable.class);
        }

        return new CacheVariables(
                intReturnCacheVar,
                longReturnCacheVar,
                floatReturnCacheVar,
                doubleReturnCacheVar,
                objectReturnCacheVar,
                throwableCacheVar);
    }

    private VariablesStorage allocateStorageVariableSlots(VariableTable varTable, TypeTracker storageTypes) {
        Variable intStorageVar = null;
        Variable longStorageVar = null;
        Variable floatStorageVar = null;
        Variable doubleStorageVar = null;
        Variable objectStorageVar = null;
        if (storageTypes.intFound) {
            intStorageVar = varTable.acquireExtra(int[].class);
        }
        if (storageTypes.longFound) {
            longStorageVar = varTable.acquireExtra(long[].class);
        }
        if (storageTypes.floatFound) {
            floatStorageVar = varTable.acquireExtra(float[].class);
        }
        if (storageTypes.doubleFound) {
            doubleStorageVar = varTable.acquireExtra(double[].class);
        }
        if (storageTypes.objectFound) {
            objectStorageVar = varTable.acquireExtra(Object[].class);
        }

        return new VariablesStorage(
                intStorageVar,
                longStorageVar,
                floatStorageVar,
                doubleStorageVar,
                objectStorageVar);
    }

    private LockVariables allocateLockVariableSlots(VariableTable varTable, boolean containsSyncPoints) {
        Variable lockStateVar = null;
        Variable lockCounterVar = null;
        Variable lockArrayLenVar = null;

        if (containsSyncPoints) {
            lockStateVar = varTable.acquireExtra(LockState.class);
            lockCounterVar = varTable.acquireExtra(Type.INT_TYPE);
            lockArrayLenVar = varTable.acquireExtra(Type.INT_TYPE);
        }

        return new LockVariables(lockStateVar, lockCounterVar, lockArrayLenVar);
    }

    private int getIndexOfContinuationVar(MethodNode methodNode) {
        // If it is NOT static, the first index in the local variables table is always the "this" pointer,
        // followed by the arguments passed to the method.
        // If it is static, the local variables table doesn't contain the "this" pointer, just the arguments passed in to the method.
        Type[] argumentTypes = Type.getMethodType(methodNode.desc).getArgumentTypes();

        int idx = -1;
        for (int i = 0; i < argumentTypes.length; i++) {
            Type type = argumentTypes[i];
            if (type.equals(CONTINUATION_CLASS_TYPE)) {
                if (idx == -1) {
                    idx = i;
                } else {
                    // should never really happen because we should be checking before calling this method
                    throw new IllegalArgumentException("Multiple Continuation arguments found in method " + methodNode.name);
                }
            }
        }

        return idx;
    }

}
