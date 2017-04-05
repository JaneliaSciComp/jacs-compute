package org.janelia.cont.instrument;

import org.objectweb.asm.tree.InsnList;

/**
 * Instruction list generator.
 */
interface InstructionGenerator {
    /**
     * Generate instruction list.
     * @return instruction list
     */
    InsnList generate();
}
