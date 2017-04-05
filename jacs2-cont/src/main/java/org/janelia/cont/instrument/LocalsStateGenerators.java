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

public class LocalsStateGenerators {

    static StorageSizes computeLocalsSize(Frame<BasicValue> frame) {
        // Count size required for each storage array
        int intsSize = 0;
        int longsSize = 0;
        int floatsSize = 0;
        int doublesSize = 0;
        int objectsSize = 0;
        for (int i = 0; i < frame.getLocals(); i++) {
            BasicValue basicValue = frame.getLocal(i);
            Type type = basicValue.getType();

            // If type == null, basicValue is pointing to uninitialized var -- basicValue.toString() will return '.'. This means that this
            // slot contains nothing to save. So, skip this slot if we encounter it.
            if (type == null) {
                continue;
            }

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

    static InsnList loadLocals(VariablesStorage storageVars, Frame<BasicValue> frame) {
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

        // Load the locals
        for (int i = 0; i < frame.getLocals(); i++) {
            BasicValue basicValue = frame.getLocal(i);
            Type type = basicValue.getType();

            // If type == null, basicValue is pointing to uninitialized var -- basicValue.toString() will return ".". This means that this
            // slot contains nothing to load. So, skip this slot if we encounter it (such that it will remain uninitialized).
            if (type == null) {
                continue;
            }

            // If type is 'Lnull;', this means that the slot has been assigned null and that "there has been no merge yet that would 'raise'
            // the type toward some class or interface type" (from ASM mailing list). We know this slot will always contain null at this
            // point in the code so there's no specific value to load up from the array. Instead we push a null in to that slot, thereby
            // keeping the same 'Lnull;' type originally assigned to that slot (it doesn't make sense to do a CHECKCAST because 'null' is
            // not a real class and can never be a real class -- null is a reserved word in Java).
            if (type.getSort() == Type.OBJECT && "Lnull;".equals(type.getDescriptor())) {
                ret.add(new InsnNode(Opcodes.ACONST_NULL));
                ret.add(new VarInsnNode(Opcodes.ASTORE, i));
                continue;
            }

            // Load the locals
            switch (type.getSort()) {
                case Type.BOOLEAN:
                case Type.BYTE:
                case Type.SHORT:
                case Type.CHAR:
                case Type.INT:
                    ret.add(new VarInsnNode(Opcodes.ALOAD, intsVar.getIndex()));    // [int[]]
                    ret.add(new LdcInsnNode(intsCounter));                          // [int[], idx]
                    ret.add(new InsnNode(Opcodes.IALOAD));                          // [val]
                    ret.add(new VarInsnNode(Opcodes.ISTORE, i));                    // []
                    intsCounter++;
                    break;
                case Type.FLOAT:
                    ret.add(new VarInsnNode(Opcodes.ALOAD, floatsVar.getIndex()));  // [float[]]
                    ret.add(new LdcInsnNode(floatsCounter));                        // [float[], idx]
                    ret.add(new InsnNode(Opcodes.FALOAD));                          // [val]
                    ret.add(new VarInsnNode(Opcodes.FSTORE, i));                    // []
                    floatsCounter++;
                    break;
                case Type.LONG:
                    ret.add(new VarInsnNode(Opcodes.ALOAD, longsVar.getIndex()));   // [long[]]
                    ret.add(new LdcInsnNode(longsCounter));                         // [long[], idx]
                    ret.add(new InsnNode(Opcodes.LALOAD));                          // [val_PART1, val_PART2]
                    ret.add(new VarInsnNode(Opcodes.LSTORE, i));                    // []
                    longsCounter++;
                    break;
                case Type.DOUBLE:
                    ret.add(new VarInsnNode(Opcodes.ALOAD, doublesVar.getIndex())); // [double[]]
                    ret.add(new LdcInsnNode(doublesCounter));                       // [double[], idx]
                    ret.add(new InsnNode(Opcodes.DALOAD));                          // [val_PART1, val_PART2]
                    ret.add(new VarInsnNode(Opcodes.DSTORE, i));                    // []
                    doublesCounter++;
                    break;
                case Type.ARRAY:
                case Type.OBJECT:
                    ret.add(new VarInsnNode(Opcodes.ALOAD, objectsVar.getIndex())); // [Object[]]
                    ret.add(new LdcInsnNode(objectsCounter));                       // [Object[], idx]
                    ret.add(new InsnNode(Opcodes.AALOAD));                          // [val]
                    // must cast, otherwise the jvm won't know the type that's in the localvariable slot and it'll fail when the code tries
                    // to access a method/field on it
                    ret.add(new TypeInsnNode(Opcodes.CHECKCAST, basicValue.getType().getInternalName()));
                    ret.add(new VarInsnNode(Opcodes.ASTORE, i));                    // []
                    objectsCounter++;
                    break;
                case Type.METHOD:
                case Type.VOID:
                default:
                    throw new IllegalStateException();
            }
        }

        return ret;
    }

    static InsnList saveLocals(VariablesStorage storageVars, Frame<BasicValue> frame) {
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

        StorageSizes storageSizes = computeLocalsSize(frame);

        InsnList ret = new InsnList();

        // Create storage arrays and save them in respective storage vars
        ret.add(BasicCodeGenerators.merge(
                BasicCodeGenerators.mergeIf(intsVar != null, () -> new Object[]{
                        new LdcInsnNode(storageSizes.getIntsSize()),
                        new IntInsnNode(Opcodes.NEWARRAY, Opcodes.T_INT),
                        new VarInsnNode(Opcodes.ASTORE, intsVar.getIndex())
                }),
                BasicCodeGenerators.mergeIf(floatsVar != null, () -> new Object[]{
                        new LdcInsnNode(storageSizes.getFloatsSize()),
                        new IntInsnNode(Opcodes.NEWARRAY, Opcodes.T_FLOAT),
                        new VarInsnNode(Opcodes.ASTORE, floatsVar.getIndex())
                }),
                BasicCodeGenerators.mergeIf(longsVar != null, () -> new Object[]{
                        new LdcInsnNode(storageSizes.getLongsSize()),
                        new IntInsnNode(Opcodes.NEWARRAY, Opcodes.T_LONG),
                        new VarInsnNode(Opcodes.ASTORE, longsVar.getIndex())
                }),
                BasicCodeGenerators.mergeIf(doublesVar != null, () -> new Object[]{
                        new LdcInsnNode(storageSizes.getDoublesSize()),
                        new IntInsnNode(Opcodes.NEWARRAY, Opcodes.T_DOUBLE),
                        new VarInsnNode(Opcodes.ASTORE, doublesVar.getIndex())
                }),
                BasicCodeGenerators.mergeIf(objectsVar != null, () -> new Object[]{
                        new LdcInsnNode(storageSizes.getObjectsSize()),
                        new TypeInsnNode(Opcodes.ANEWARRAY, "java/lang/Object"),
                        new VarInsnNode(Opcodes.ASTORE, objectsVar.getIndex())
                })
        ));

        // Save the locals
        for (int i = 0; i < frame.getLocals(); i++) {
            BasicValue basicValue = frame.getLocal(i);
            Type type = basicValue.getType();

            // If type == null, basicValue is pointing to uninitialized var -- basicValue.toString() will return '.'. This means that this
            // slot contains nothing to save. So, skip this slot if we encounter it.
            if (type == null) {
                continue;
            }

            // If type is 'Lnull;', this means that the slot has been assigned null and that "there has been no merge yet that would 'raise'
            // the type toward some class or interface type" (from ASM mailing list). We know this slot will always contain null at this
            // point in the code so we can avoid saving it. When we load it back up, we can simply push a null in to that slot, thereby
            // keeping the same 'Lnull;' type.
            if ("Lnull;".equals(type.getDescriptor())) {
                continue;
            }

            // Place item in to appropriate storage array
            switch (type.getSort()) {
                case Type.BOOLEAN:
                case Type.BYTE:
                case Type.SHORT:
                case Type.CHAR:
                case Type.INT:
                    ret.add(new VarInsnNode(Opcodes.ALOAD, intsVar.getIndex())); // [int[]]
                    ret.add(new LdcInsnNode(intsCounter));                       // [int[], idx]
                    ret.add(new VarInsnNode(Opcodes.ILOAD, i));                  // [int[], idx, val]
                    ret.add(new InsnNode(Opcodes.IASTORE));                      // []
                    intsCounter++;
                    break;
                case Type.FLOAT:
                    ret.add(new VarInsnNode(Opcodes.ALOAD, floatsVar.getIndex())); // [float[]]
                    ret.add(new LdcInsnNode(floatsCounter));                       // [float[], idx]
                    ret.add(new VarInsnNode(Opcodes.FLOAD, i));                    // [float[], idx, val]
                    ret.add(new InsnNode(Opcodes.FASTORE));                        // []
                    floatsCounter++;
                    break;
                case Type.LONG:
                    ret.add(new VarInsnNode(Opcodes.ALOAD, longsVar.getIndex())); // [long[]]
                    ret.add(new LdcInsnNode(longsCounter));                       // [long[], idx]
                    ret.add(new VarInsnNode(Opcodes.LLOAD, i));                   // [long[], idx, val]
                    ret.add(new InsnNode(Opcodes.LASTORE));                       // []
                    longsCounter++;
                    break;
                case Type.DOUBLE:
                    ret.add(new VarInsnNode(Opcodes.ALOAD, doublesVar.getIndex())); // [double[]]
                    ret.add(new LdcInsnNode(doublesCounter));                       // [double[], idx]
                    ret.add(new VarInsnNode(Opcodes.DLOAD, i));                     // [double[], idx, val]
                    ret.add(new InsnNode(Opcodes.DASTORE));                         // []
                    doublesCounter++;
                    break;
                case Type.ARRAY:
                case Type.OBJECT:
                    ret.add(new VarInsnNode(Opcodes.ALOAD, objectsVar.getIndex())); // [Object[]]
                    ret.add(new LdcInsnNode(objectsCounter));                       // [Object[], idx]
                    ret.add(new VarInsnNode(Opcodes.ALOAD, i));                     // [Object[], idx, val]
                    ret.add(new InsnNode(Opcodes.AASTORE));                         // []
                    objectsCounter++;
                    break;
                case Type.METHOD:
                case Type.VOID:
                default:
                    throw new IllegalStateException();
            }
        }
        return ret;
    }

}
