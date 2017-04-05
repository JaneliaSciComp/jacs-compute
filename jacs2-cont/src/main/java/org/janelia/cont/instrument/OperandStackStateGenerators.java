package org.janelia.cont.instrument;

import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.InsnList;
import org.objectweb.asm.tree.InsnNode;
import org.objectweb.asm.tree.IntInsnNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.TypeInsnNode;
import org.objectweb.asm.tree.VarInsnNode;
import org.objectweb.asm.tree.analysis.BasicValue;
import org.objectweb.asm.tree.analysis.Frame;

class OperandStackStateGenerators {

    static StorageSizes computeStackSize(Frame<BasicValue> frame, int offset, int length) {
        // Count size required for each storage array
        int intsSize = 0;
        int longsSize = 0;
        int floatsSize = 0;
        int doublesSize = 0;
        int objectsSize = 0;
        for (int i = offset + length - 1; i >= offset; i--) {
            BasicValue basicValue = frame.getStack(i);
            Type type = basicValue.getType();

            // If type is 'Lnull;', this means that the slot has been assigned null and that "there has been no merge yet that would 'raise'
            // the type toward some class or interface type" (from ASM mailing list). We know this slot will always contain null at this
            // point in the code so we can avoid saving it. When we load it back up, we can simply push a null in to that slot, thereby
            // keeping the same 'Lnull;' type.
            if ("Lnull;".equals(type.getDescriptor())) {
                continue;
            }

            switch (type.getSort()) {
                case Type.BOOLEAN:
                case Type.BYTE:
                case Type.SHORT:
                case Type.CHAR:
                case Type.INT:
                    intsSize++;
                    break;
                case Type.FLOAT:
                    floatsSize++;
                    break;
                case Type.LONG:
                    longsSize++;
                    break;
                case Type.DOUBLE:
                    doublesSize++;
                    break;
                case Type.ARRAY:
                case Type.OBJECT:
                    objectsSize++;
                    break;
                case Type.METHOD:
                case Type.VOID:
                default:
                    throw new IllegalStateException();
            }
        }

        return new StorageSizes(intsSize, longsSize, floatsSize, doublesSize, objectsSize);
    }

    static InsnList loadOperandStack(VariablesStorage storageVars, Frame<BasicValue> frame) {
        return loadOperandStack(storageVars, frame, 0, 0, frame.getStackSize());
    }

    static InsnList loadOperandStack(VariablesStorage storageVars, Frame<BasicValue> frame,
                                     int storageStackStartIdx,  // stack idx which the storage was started at
                                     int storageStackLoadIdx,   // stack idx we should start loading at
                                     int count) {
        Variable intsVar = storageVars.getIntStorageVar();
        Variable floatsVar = storageVars.getFloatStorageVar();
        Variable longsVar = storageVars.getLongStorageVar();
        Variable doublesVar = storageVars.getDoubleStorageVar();
        Variable objectsVar = storageVars.getObjectStorageVar();

        int intsCounter = 0;
        int floatsCounter = 0;
        int longsCounter = 0;
        int doublesCounter = 0;
        int objectsCounter = 0;

        InsnList ret = new InsnList();

        // Increment offsets for parts of the storage arrays we don't care about. We need to do this so when we load we're loading from the
        // correct offsets in the storage arrays
        for (int i = storageStackStartIdx; i < storageStackLoadIdx; i++) {
            BasicValue basicValue = frame.getStack(i);
            Type type = basicValue.getType();

            // If type is 'Lnull;', this means that the slot has been assigned null and that "there has been no merge yet that would 'raise'
            // the type toward some class or interface type" (from ASM mailing list). We know this slot will always contain null at this
            // point in the code so there's no specific value to load up from the array.
            if (type.getSort() == Type.OBJECT && "Lnull;".equals(type.getDescriptor())) {
                continue; // skip
            }

            switch (type.getSort()) {
                case Type.BOOLEAN:
                case Type.BYTE:
                case Type.SHORT:
                case Type.CHAR:
                case Type.INT:
                    intsCounter++;
                    break;
                case Type.FLOAT:
                    floatsCounter++;
                    break;
                case Type.LONG:
                    longsCounter++;
                    break;
                case Type.DOUBLE:
                    doublesCounter++;
                    break;
                case Type.ARRAY:
                case Type.OBJECT:
                    objectsCounter++;
                    break;
                case Type.METHOD:
                case Type.VOID:
                default:
                    throw new IllegalArgumentException();
            }
        }
        // Restore the stack
        for (int i = storageStackLoadIdx; i < storageStackLoadIdx + count; i++) {
            BasicValue basicValue = frame.getStack(i);
            Type type = basicValue.getType();

            // If type is 'Lnull;', this means that the slot has been assigned null and that "there has been no merge yet that would 'raise'
            // the type toward some class or interface type" (from ASM mailing list). We know this slot will always contain null at this
            // point in the code so there's no specific value to load up from the array. Instead we push a null in to that slot, thereby
            // keeping the same 'Lnull;' type originally assigned to that slot (it doesn't make sense to do a CHECKCAST because 'null' is
            // not a real class and can never be a real class -- null is a reserved word in Java).
            if (type.getSort() == Type.OBJECT && "Lnull;".equals(type.getDescriptor())) {
                ret.add(new InsnNode(Opcodes.ACONST_NULL));
                continue;
            }

            // Load item from stack storage array
            // Convert the item to an object (if not already an object) and stores it in local vars table. Item removed from stack.
            switch (type.getSort()) {
                case Type.BOOLEAN:
                case Type.BYTE:
                case Type.SHORT:
                case Type.CHAR:
                case Type.INT:
                    ret.add(new VarInsnNode(Opcodes.ALOAD, intsVar.getIndex())); // [int[]]
                    ret.add(new LdcInsnNode(intsCounter));                       // [int[], idx]
                    ret.add(new InsnNode(Opcodes.IALOAD));                       // [val]
                    intsCounter++;
                    break;
                case Type.FLOAT:
                    ret.add(new VarInsnNode(Opcodes.ALOAD, floatsVar.getIndex())); // [float[]]
                    ret.add(new LdcInsnNode(floatsCounter));                       // [float[], idx]
                    ret.add(new InsnNode(Opcodes.FALOAD));                         // [val]
                    floatsCounter++;
                    break;
                case Type.LONG:
                    ret.add(new VarInsnNode(Opcodes.ALOAD, longsVar.getIndex()));  // [long[]]
                    ret.add(new LdcInsnNode(longsCounter));                        // [long[], idx]
                    ret.add(new InsnNode(Opcodes.LALOAD));                         // [val_PART1, val_PART2]
                    longsCounter++;
                    break;
                case Type.DOUBLE:
                    ret.add(new VarInsnNode(Opcodes.ALOAD, doublesVar.getIndex()));  // [double[]]
                    ret.add(new LdcInsnNode(doublesCounter));                        // [double[], idx]
                    ret.add(new InsnNode(Opcodes.DALOAD));                           // [val_PART1, val_PART2]
                    doublesCounter++;
                    break;
                case Type.ARRAY:
                case Type.OBJECT:
                    ret.add(new VarInsnNode(Opcodes.ALOAD, objectsVar.getIndex())); // [Object[]]
                    ret.add(new LdcInsnNode(objectsCounter));                       // [Object[], idx]
                    ret.add(new InsnNode(Opcodes.AALOAD));                          // [val]
                    ret.add(new TypeInsnNode(Opcodes.CHECKCAST, basicValue.getType().getInternalName()));
                    objectsCounter++;
                    break;
                case Type.METHOD:
                case Type.VOID:
                default:
                    throw new IllegalArgumentException();
            }
        }
        return ret;
    }

    static InsnList saveOperandStack(VariablesStorage storageVars, Frame<BasicValue> frame) {
        return saveOperandStack(storageVars, frame, frame.getStackSize());
    }

    /**
     * This method should save the stack without clearing it.
     */
    static InsnList saveOperandStack(VariablesStorage storageVars, Frame<BasicValue> frame, int count) {
        Variable intsVar = storageVars.getIntStorageVar();
        Variable floatsVar = storageVars.getFloatStorageVar();
        Variable longsVar = storageVars.getLongStorageVar();
        Variable doublesVar = storageVars.getDoubleStorageVar();
        Variable objectsVar = storageVars.getObjectStorageVar();

        StorageSizes storageSizes = computeStackSize(frame, frame.getStackSize() - count, count);

        int intsCounter = storageSizes.getIntsSize() - 1;
        int floatsCounter = storageSizes.getFloatsSize() - 1;
        int longsCounter = storageSizes.getLongsSize() - 1;
        int doublesCounter = storageSizes.getDoublesSize() - 1;
        int objectsCounter = storageSizes.getObjectsSize() - 1;

        InsnList ret = new InsnList();

        // Create stack storage arrays and save them
        ret.add(BasicCodeGenerators.merge(
                BasicCodeGenerators.mergeIf(storageSizes.getIntsSize() > 0, () -> new Object[]{
                        new LdcInsnNode(storageSizes.getIntsSize()),
                        new IntInsnNode(Opcodes.NEWARRAY, Opcodes.T_INT),
                        new VarInsnNode(Opcodes.ASTORE, intsVar.getIndex())
                }),
                BasicCodeGenerators.mergeIf(storageSizes.getFloatsSize() > 0, () -> new Object[]{
                        new LdcInsnNode(storageSizes.getFloatsSize()),
                        new IntInsnNode(Opcodes.NEWARRAY, Opcodes.T_FLOAT),
                        new VarInsnNode(Opcodes.ASTORE, floatsVar.getIndex())
                }),
                BasicCodeGenerators.mergeIf(storageSizes.getLongsSize() > 0, () -> new Object[]{
                        new LdcInsnNode(storageSizes.getLongsSize()),
                        new IntInsnNode(Opcodes.NEWARRAY, Opcodes.T_LONG),
                        new VarInsnNode(Opcodes.ASTORE, longsVar.getIndex())
                }),
                BasicCodeGenerators.mergeIf(storageSizes.getDoublesSize() > 0, () -> new Object[]{
                        new LdcInsnNode(storageSizes.getDoublesSize()),
                        new IntInsnNode(Opcodes.NEWARRAY, Opcodes.T_DOUBLE),
                        new VarInsnNode(Opcodes.ASTORE, doublesVar.getIndex())
                }),
                BasicCodeGenerators.mergeIf(storageSizes.getObjectsSize() > 0, () -> new Object[]{
                        new LdcInsnNode(storageSizes.getObjectsSize()),
                        new TypeInsnNode(Opcodes.ANEWARRAY, "java/lang/Object"),
                        new VarInsnNode(Opcodes.ASTORE, objectsVar.getIndex())
                })
        ));

        // Save the stack
        int start = frame.getStackSize() - 1;
        int end = frame.getStackSize() - count;
        for (int i = start; i >= end; i--) {
            BasicValue basicValue = frame.getStack(i);
            Type type = basicValue.getType();

            // If type is 'Lnull;', this means that the slot has been assigned null and that "there has been no merge yet that would 'raise'
            // the type toward some class or interface type" (from ASM mailing list). We know this slot will always contain null at this
            // point in the code so we can avoid saving it (but we still need to do a POP to get rid of it). When we load it back up, we can
            // simply push a null in to that slot, thereby keeping the same 'Lnull;' type.
            if ("Lnull;".equals(type.getDescriptor())) {
                ret.add(new InsnNode(Opcodes.POP));
                continue;
            }

            // Convert the item to an object (if not already an object) and stores it in local vars table. Item removed from stack.
            switch (type.getSort()) {
                case Type.BOOLEAN:
                case Type.BYTE:
                case Type.SHORT:
                case Type.CHAR:
                case Type.INT:
                    ret.add(new VarInsnNode(Opcodes.ALOAD, intsVar.getIndex())); // [val, int[]]
                    ret.add(new InsnNode(Opcodes.SWAP));                         // [int[], val]
                    ret.add(new LdcInsnNode(intsCounter));                       // [int[], val, idx]
                    ret.add(new InsnNode(Opcodes.SWAP));                         // [int[], idx, val]
                    ret.add(new InsnNode(Opcodes.IASTORE));                      // []
                    intsCounter--;
                    break;
                case Type.FLOAT:
                    ret.add(new VarInsnNode(Opcodes.ALOAD, floatsVar.getIndex())); // [val, float[]]
                    ret.add(new InsnNode(Opcodes.SWAP));                           // [float[], val]
                    ret.add(new LdcInsnNode(floatsCounter));                       // [float[], val, idx]
                    ret.add(new InsnNode(Opcodes.SWAP));                           // [float[], idx, val]
                    ret.add(new InsnNode(Opcodes.FASTORE));                        // []
                    floatsCounter--;
                    break;
                case Type.LONG:
                    ret.add(new VarInsnNode(Opcodes.ALOAD, longsVar.getIndex()));  // [val_PART1, val_PART2, long[]]
                    ret.add(new LdcInsnNode(longsCounter));                        // [val_PART1, val_PART2, long[], idx]
                    ret.add(new InsnNode(Opcodes.DUP2_X2));                        // [long[], idx, val_PART1, val_PART2, long[], idx]
                    ret.add(new InsnNode(Opcodes.POP2));                           // [long[], idx, val_PART1, val_PART2]
                    ret.add(new InsnNode(Opcodes.LASTORE));                        // []
                    longsCounter--;
                    break;
                case Type.DOUBLE:
                    ret.add(new VarInsnNode(Opcodes.ALOAD, doublesVar.getIndex()));  // [val_PART1, val_PART2, double[]]
                    ret.add(new LdcInsnNode(doublesCounter));                        // [val_PART1, val_PART2, double[], idx]
                    ret.add(new InsnNode(Opcodes.DUP2_X2));                          // [double[], idx, val_PART1, val_PART2, double[], idx]
                    ret.add(new InsnNode(Opcodes.POP2));                             // [double[], idx, val_PART1, val_PART2]
                    ret.add(new InsnNode(Opcodes.DASTORE));                          // []
                    doublesCounter--;
                    break;
                case Type.ARRAY:
                case Type.OBJECT:
                    ret.add(new VarInsnNode(Opcodes.ALOAD, objectsVar.getIndex())); // [val, object[]]
                    ret.add(new InsnNode(Opcodes.SWAP));                            // [object[], val]
                    ret.add(new LdcInsnNode(objectsCounter));                       // [object[], val, idx]
                    ret.add(new InsnNode(Opcodes.SWAP));                            // [object[], idx, val]
                    ret.add(new InsnNode(Opcodes.AASTORE));                         // []
                    objectsCounter--;
                    break;
                case Type.METHOD:
                case Type.VOID:
                default:
                    throw new IllegalArgumentException();
            }
        }

        return ret;
    }

}
