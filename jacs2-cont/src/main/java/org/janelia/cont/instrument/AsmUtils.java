package org.janelia.cont.instrument;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.Validate;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.AnnotationNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldNode;
import org.objectweb.asm.tree.InsnList;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.LabelNode;
import org.objectweb.asm.tree.LineNumberNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.TryCatchBlockNode;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

class AsmUtils {
    static FieldNode findField(ClassNode classNode, String name) {
        Validate.notNull(classNode);
        Validate.notNull(name);
        Validate.notEmpty(name);
        List<FieldNode> classFields = classNode.fields;
        return  classFields.stream()
                .filter(field -> name.equals(field.name))
                .findAny()
                .orElse(null);
    }

    static List<MethodNode> findMethodsWithAnnotation(Collection<MethodNode> methodNodes, Type annotationType) {
        Validate.notNull(methodNodes);
        Validate.notNull(annotationType);
        Validate.noNullElements(methodNodes);

        return methodNodes.stream().filter(mn -> {
            Optional<AnnotationNode> res = lookupAnnotation(mn.visibleAnnotations, annotationType);
            if (res.isPresent()) {
                return true;
            } else {
                return lookupAnnotation(mn.invisibleAnnotations, annotationType).isPresent();
            }
        }).collect(Collectors.toList());
    }

    private static Optional<AnnotationNode> lookupAnnotation(List<AnnotationNode> annotationNodes, Type annotationType) {
        if (annotationNodes != null) {
            return annotationNodes.stream().filter(an -> {
                return an.desc.equals(annotationType.getDescriptor());
            }).findAny();
        } else {
            return Optional.empty();
        }
    }

    static List<AbstractInsnNode> findInvocationsOf(InsnList instrList, Method expectedMethod) {
        List<AbstractInsnNode> ret = new ArrayList<>();

        Type expectedMethodDesc = Type.getType(expectedMethod);
        Type expectedMethodOwner = Type.getType(expectedMethod.getDeclaringClass());
        String expectedMethodName = expectedMethod.getName();

        for (Iterator<AbstractInsnNode> it = instrList.iterator(); it.hasNext(); ) {
            AbstractInsnNode instructionNode = it.next();
            Type methodDesc;
            Type methodOwner;
            String methodName;
            if (instructionNode instanceof MethodInsnNode) {
                MethodInsnNode methodInsnNode = (MethodInsnNode) instructionNode;
                methodDesc = Type.getType(methodInsnNode.desc);
                methodOwner = Type.getObjectType(methodInsnNode.owner);
                methodName = methodInsnNode.name;
            } else {
                continue;
            }

            if (methodDesc.equals(expectedMethodDesc) && methodOwner.equals(expectedMethodOwner) && methodName.equals(expectedMethodName)) {
                ret.add(instructionNode);
            }
        }

        return ret;
    }

    static Optional<LineNumberNode> findLineNumberForInstruction(InsnList insnList, AbstractInsnNode insnNode) {
        int idx = insnList.indexOf(insnNode);
        Validate.isTrue(idx != -1);

        // Get index of labels and insnNode within method
        ListIterator<AbstractInsnNode> insnIt = insnList.iterator(idx);
        while (insnIt.hasPrevious()) {
            AbstractInsnNode node = insnIt.previous();

            if (node instanceof LineNumberNode) {
                return Optional.of((LineNumberNode) node);
            }
        }

        return Optional.empty();
    }

    static List<AbstractInsnNode> findInvocationsWithParameter(InsnList insnList, Type expectedParamType) {
        List<AbstractInsnNode> ret = new ArrayList<>();

        Iterator<AbstractInsnNode> it = insnList.iterator();
        while (it.hasNext()) {
            AbstractInsnNode instructionNode = it.next();
            Type[] methodParamTypes;
            if (instructionNode instanceof MethodInsnNode) {
                MethodInsnNode methodInsnNode = (MethodInsnNode) instructionNode;
                Type methodType = Type.getType(methodInsnNode.desc);
                methodParamTypes = methodType.getArgumentTypes();
            } else if (instructionNode instanceof InvokeDynamicInsnNode) {
                InvokeDynamicInsnNode invokeDynamicInsnNode = (InvokeDynamicInsnNode) instructionNode;
                Type methodType = Type.getType(invokeDynamicInsnNode.desc);
                methodParamTypes = methodType.getArgumentTypes();
            } else {
                continue;
            }
            if (Arrays.asList(methodParamTypes).contains(expectedParamType)) {
                ret.add(instructionNode);
            }
        }

        return ret;
    }

    static List<TryCatchBlockNode> findTryCatchBlockNodesEncompassingInstruction(InsnList insnList, List<TryCatchBlockNode> tryCatchBlockNodes, AbstractInsnNode insnNode) {
        Map<LabelNode, Integer> labelPositions = new HashMap<>();
        int insnNodeIdx = -1;

        // Get index of labels and insnNode within method
        ListIterator<AbstractInsnNode> insnIt = insnList.iterator();
        int insnCounter = 0;
        while (insnIt.hasNext()) {
            AbstractInsnNode node = insnIt.next();

            // If our instruction, save index
            if (node == insnNode) {
                if (insnNodeIdx == -1) {
                    insnNodeIdx = insnCounter;
                } else {
                    throw new IllegalArgumentException(); // insnNode encountered multiple times in methodNode. Should not happen.
                }
            }

            // If label node, save position
            if (node instanceof LabelNode) {
                labelPositions.put((LabelNode) node, insnCounter);
            }

            // Increment counter
            insnCounter++;
        }

        // Find out which trycatch blocks insnNode is within
        List<TryCatchBlockNode> ret = new ArrayList<>();
        for (TryCatchBlockNode tryCatchBlockNode : tryCatchBlockNodes) {
            Integer startIdx = labelPositions.get(tryCatchBlockNode.start);
            Integer endIdx = labelPositions.get(tryCatchBlockNode.end);

            Validate.isTrue(startIdx != null);
            Validate.isTrue(endIdx != null);

            if (insnNodeIdx >= startIdx && insnNodeIdx < endIdx) {
                ret.add(tryCatchBlockNode);
            }
        }

        return ret;
    }

    static List<AbstractInsnNode> findOpcodes(InsnList insnList, Integer... opcodes) {
        List<AbstractInsnNode> ret = new LinkedList<>();

        Set<Integer> opcodeSet = ImmutableSet.copyOf(opcodes);

        Iterator<AbstractInsnNode> it = insnList.iterator();
        while (it.hasNext()) {
            AbstractInsnNode insnNode = it.next();
            if (opcodeSet.contains(insnNode.getOpcode())) {
                ret.add(insnNode);
            }
        }

        return ret;
    }

    static Type getReturnTypeOfInvocation(AbstractInsnNode invokeNode) {
        if (invokeNode instanceof MethodInsnNode) {
            MethodInsnNode methodInsnNode = (MethodInsnNode) invokeNode;
            Type methodType = Type.getType(methodInsnNode.desc);
            return methodType.getReturnType();
        } else if (invokeNode instanceof InvokeDynamicInsnNode) {
            InvokeDynamicInsnNode invokeDynamicInsnNode = (InvokeDynamicInsnNode) invokeNode;
            Type methodType = Type.getType(invokeDynamicInsnNode.desc);
            return methodType.getReturnType();
        } else {
            throw new IllegalArgumentException();
        }
    }

}
