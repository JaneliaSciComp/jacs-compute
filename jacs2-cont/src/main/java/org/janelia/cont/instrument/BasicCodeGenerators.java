package org.janelia.cont.instrument;

import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.IincInsnNode;
import org.objectweb.asm.tree.InsnList;
import org.objectweb.asm.tree.InsnNode;
import org.objectweb.asm.tree.JumpInsnNode;
import org.objectweb.asm.tree.LabelNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.LineNumberNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.TypeInsnNode;
import org.objectweb.asm.tree.VarInsnNode;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.function.Supplier;

public class BasicCodeGenerators {

    static InsnList addLabel(LabelNode labelNode) {
        InsnList ret = new InsnList();
        ret.add(labelNode);
        return ret;
    }

    static InsnList call(Method method, InsnList ... args) {
        InsnList ret = new InsnList();

        for (InsnList arg : args) {
            ret.add(arg);
        }

        Type clsType = Type.getType(method.getDeclaringClass());
        Type methodType = Type.getType(method);

        if ((method.getModifiers() & Modifier.STATIC) == Modifier.STATIC) {
            ret.add(new MethodInsnNode(Opcodes.INVOKESTATIC,
                    clsType.getInternalName(),
                    method.getName(),
                    methodType.getDescriptor(),
                    false));
        } else if (method.getDeclaringClass().isInterface()) {
            ret.add(new MethodInsnNode(Opcodes.INVOKEINTERFACE,
                    clsType.getInternalName(),
                    method.getName(),
                    methodType.getDescriptor(),
                    true));
        } else {
            ret.add(new MethodInsnNode(Opcodes.INVOKEVIRTUAL,
                    clsType.getInternalName(),
                    method.getName(),
                    methodType.getDescriptor(),
                    false));
        }
        return ret;
    }

    static InsnList construct(Constructor<?> constructor, InsnList ... args) {
        InsnList ret = new InsnList();

        Type clsType = Type.getType(constructor.getDeclaringClass());
        Type methodType = Type.getType(constructor);

        ret.add(new TypeInsnNode(Opcodes.NEW, clsType.getInternalName()));
        ret.add(new InsnNode(Opcodes.DUP));
        for (InsnList arg : args) {
            ret.add(arg);
        }
        ret.add(new MethodInsnNode(Opcodes.INVOKESPECIAL, clsType.getInternalName(), "<init>", methodType.getDescriptor(), false));

        return ret;
    }

    static InsnList forEach(Variable counterVar, Variable arrayLenVar, InsnList array, InsnList action) {
        InsnList ret = new InsnList();
        LabelNode doneLabelNode = new LabelNode();
        LabelNode loopLabelNode = new LabelNode();

        // put zero in to counterVar
        ret.add(new LdcInsnNode(0)); // int
        ret.add(new VarInsnNode(Opcodes.ISTORE, counterVar.getIndex())); //

        // load array we'll be traversing over
        ret.add(array); // object[]

        // put array length in to arrayLenVar
        ret.add(new InsnNode(Opcodes.DUP)); // object[], object[]
        ret.add(new InsnNode(Opcodes.ARRAYLENGTH)); // object[], int
        ret.add(new VarInsnNode(Opcodes.ISTORE, arrayLenVar.getIndex())); // object[]

        // loopLabelNode: test if counterVar == arrayLenVar, if it does then jump to doneLabelNode
        ret.add(loopLabelNode);
        ret.add(new VarInsnNode(Opcodes.ILOAD, counterVar.getIndex())); // object[], int
        ret.add(new VarInsnNode(Opcodes.ILOAD, arrayLenVar.getIndex())); // object[], int, int
        ret.add(new JumpInsnNode(Opcodes.IF_ICMPEQ, doneLabelNode)); // object[]

        // load object from object[]
        ret.add(new InsnNode(Opcodes.DUP)); // object[], object[]
        ret.add(new VarInsnNode(Opcodes.ILOAD, counterVar.getIndex())); // object[], object[], int
        ret.add(new InsnNode(Opcodes.AALOAD)); // object[], object

        // call action
        ret.add(action); // object[]

        // increment counter var and goto loopLabelNode
        ret.add(new IincInsnNode(counterVar.getIndex(), 1)); // object[]
        ret.add(new JumpInsnNode(Opcodes.GOTO, loopLabelNode)); // object[]

        // doneLabelNode: pop object[] off of stack
        ret.add(doneLabelNode);
        ret.add(new InsnNode(Opcodes.POP)); //

        return ret;
    }

    static InsnList generateLineNumber(int num) {
        InsnList ret = new InsnList();

        LabelNode labelNode = new LabelNode();
        ret.add(labelNode);
        ret.add(new LineNumberNode(num, labelNode));

        return ret;
    }

    static InsnList loadIntConst(int i) {
        InsnList ret = new InsnList();
        ret.add(new LdcInsnNode(i));
        return ret;
    }

    static InsnList loadNull() {
        InsnList ret = new InsnList();
        ret.add(new InsnNode(Opcodes.ACONST_NULL));
        return ret;
    }

    static InsnList loadVar(Variable variable) {
        InsnList ret = new InsnList();
        switch (variable.getType().getSort()) {
            case Type.BOOLEAN:
            case Type.BYTE:
            case Type.CHAR:
            case Type.SHORT:
            case Type.INT:
                ret.add(new VarInsnNode(Opcodes.ILOAD, variable.getIndex()));
                break;
            case Type.LONG:
                ret.add(new VarInsnNode(Opcodes.LLOAD, variable.getIndex()));
                break;
            case Type.FLOAT:
                ret.add(new VarInsnNode(Opcodes.FLOAD, variable.getIndex()));
                break;
            case Type.DOUBLE:
                ret.add(new VarInsnNode(Opcodes.DLOAD, variable.getIndex()));
                break;
            case Type.OBJECT:
            case Type.ARRAY:
                ret.add(new VarInsnNode(Opcodes.ALOAD, variable.getIndex()));
                break;
            default:
                throw new IllegalStateException();
        }

        return ret;
    }

    static InsnList merge(Object... insns) {
        InsnList ret = new InsnList();
        for (Object insn : insns) {
            if (insn instanceof AbstractInsnNode) {
                // add single instruction
                AbstractInsnNode insnNode = (AbstractInsnNode) insn;
                ret.add(insnNode);
            } else if (insn instanceof InsnList) {
                // add instruction list
                InsnList insnList = (InsnList) insn;
                ret.add(insnList);
            } else if (insn instanceof InstructionGenerator) {
                // generate conditional merger instruction list and add
                InsnList insnList = ((InstructionGenerator) insn).generate();
                ret.add(insnList);
            } else {
                // unrecognized
                throw new IllegalArgumentException();
            }
        }
        return ret;
    }

    static ConditionalMerger mergeIf(boolean condition, Supplier<Object[]> insnsSupplier) {
        ConditionalMerger merger = new ConditionalMerger();
        return merger.mergeIf(condition, insnsSupplier);
    }


}
