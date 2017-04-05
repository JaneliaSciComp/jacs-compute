package org.janelia.cont;

public class Continuation {
    private static final long serialVersionUID = 101L;

    private MethodState firstCheckpoint;
    private MethodState nextLoadPointer;
    private MethodState nextUnloadPointer;

    private MethodState firstCutpointPointer;

    public void checkpoint() {
    }

    public void restore() {
    }

    public void saveCheckpoint(MethodState methodState) {
        methodState.setNext(firstCutpointPointer);
        firstCutpointPointer = methodState;
    }

}
