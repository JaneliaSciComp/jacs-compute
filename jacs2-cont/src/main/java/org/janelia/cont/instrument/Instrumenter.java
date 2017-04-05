package org.janelia.cont.instrument;

import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.janelia.cont.Continuation;
import org.janelia.cont.Suspendable;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldNode;
import org.objectweb.asm.tree.MethodNode;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class Instrumenter {

    private static final int INSTRUMENTED_MARKER_FIELD_ACCESS = Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_STATIC;
    private static final Type INSTRUMENTED_MARKER_FIELD_TYPE = Type.LONG_TYPE;
    private static final String INSTRUMENTED_FIELD_MARKER_NAME = "__JCONT_INSTRUMENTATION_VERSION";
    private static final Long INSTRUMENTED_MARKER_FIELD_VALUE;
    static {
        try {
            // We update serialVersionUIDs in user package whenever we do anything that makes us incompatible with previous versions, so
            // this is a good value to use to detect which version of the instrumenter we instrumented with
            INSTRUMENTED_MARKER_FIELD_VALUE = (Long) FieldUtils.readDeclaredStaticField(Continuation.class, "serialVersionUID", true);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to grab int value from " + Continuation.class.getName() + " serialVersionUID", e);
        }
    }

    private ClassInformationRepository classRepo;

    public Instrumenter(List<File> classpath) throws IOException {
        Validate.notNull(classpath);
        Validate.noNullElements(classpath);
        classRepo = FileSystemClassInformationRepository.create(classpath);
    }

    public byte[] instrument(byte[] input, InstrumentationSettings settings) {
        Validate.notNull(input);
        Validate.notNull(settings);
        Validate.isTrue(input.length > 0);

        System.out.println("!!!!!!!!!!!!!! INSTRUMENT 1");

        ClassReader cr = new ClassReader(input);
        ClassNode classNode = new InlineJSRClassNode();
        cr.accept(classNode, 0);

        // skip this if it is an interface
        if ((classNode.access & Opcodes.ACC_INTERFACE) == Opcodes.ACC_INTERFACE) {
            return input.clone();
        }

        // skip this if it has already been instrumented
        FieldNode instrumentedMarkerField = AsmUtils.findField(classNode, INSTRUMENTED_FIELD_MARKER_NAME);
        if (instrumentedMarkerField != null) {
            if (INSTRUMENTED_MARKER_FIELD_ACCESS != instrumentedMarkerField.access) {
                throw new IllegalArgumentException("Instrumentation marker found with wrong access: " + instrumentedMarkerField.access);
            }
            if (!INSTRUMENTED_MARKER_FIELD_TYPE.getDescriptor().equals(instrumentedMarkerField.desc)) {
                throw new IllegalArgumentException("Instrumentation marker found with wrong type: " + instrumentedMarkerField.desc);
            }
            if (!INSTRUMENTED_MARKER_FIELD_VALUE.equals(instrumentedMarkerField.value)) {
                throw new IllegalArgumentException("Instrumentation marker found wrong value: " + instrumentedMarkerField.value);
            }

            return input.clone();
        }

        // find methods that need to be instrumented
        List<MethodNode> methodNodesToInstrument = AsmUtils.findMethodsWithAnnotation(classNode.methods, Type.getType(Suspendable.class));
        if (methodNodesToInstrument.isEmpty()) {
            return input.clone();
        }

        // Add the "instrumented" marker
        instrumentedMarkerField = new FieldNode(
                INSTRUMENTED_MARKER_FIELD_ACCESS,
                INSTRUMENTED_FIELD_MARKER_NAME,
                INSTRUMENTED_MARKER_FIELD_TYPE.getDescriptor(),
                null,
                INSTRUMENTED_MARKER_FIELD_VALUE);
        classNode.fields.add(instrumentedMarkerField);

        // Instrument each method that needs to be instrumented
        MethodAnalyzer analyzer = new MethodAnalyzer(classRepo);
        MethodInstrumenter methodInstrumenter = new MethodInstrumenter();
        for (MethodNode methodNode : methodNodesToInstrument) {
            Optional<MethodAttributes> methodAttrs = analyzer.analyze(classNode, methodNode);

            // Instrument the method if needed
            if (methodAttrs.isPresent()) {
                methodInstrumenter.instrument(methodNode, methodAttrs.get());
            }
        }

        ClassWriter cw = new SimpleClassWriter(ClassWriter.COMPUTE_MAXS | ClassWriter.COMPUTE_FRAMES, classRepo);
        classNode.accept(cw);
        return cw.toByteArray();
    }
}
