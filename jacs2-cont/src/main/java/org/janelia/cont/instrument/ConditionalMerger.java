package org.janelia.cont.instrument;

import org.objectweb.asm.tree.InsnList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

final class ConditionalMerger implements InstructionGenerator {
    private List<Object> generatedInstructions = new ArrayList<>();

    public ConditionalMerger mergeIf(boolean condition, Supplier<Object[]> insnsSupplier) {
        if (!condition) {
            return this;
        }
        generatedInstructions.addAll(Arrays.asList(insnsSupplier.get()));
        return this;
    }

    @Override
    public InsnList generate() {
        return BasicCodeGenerators.merge(generatedInstructions.toArray());
    }
}
