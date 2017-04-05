package org.janelia.cont.instrument;

import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.JSRInlinerAdapter;
import org.objectweb.asm.tree.ClassNode;

public class InlineJSRClassNode extends ClassNode {

    public InlineJSRClassNode() {
        super(Opcodes.ASM5);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        MethodVisitor origVisitor = super.visitMethod(access, name, desc, signature, exceptions);
        return new JSRInlinerAdapter(origVisitor, access, name, desc, signature, exceptions);
    }
}
