package org.janelia.cont.instrument;

public class InstrumentationSettings {

    private final boolean debugMode;

    public InstrumentationSettings(boolean debugMode) {
        this.debugMode = debugMode;
    }

    public boolean isDebugMode() {
        return debugMode;
    }
}
