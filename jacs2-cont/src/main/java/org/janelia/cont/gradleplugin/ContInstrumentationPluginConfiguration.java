package org.janelia.cont.gradleplugin;

import org.apache.commons.lang3.Validate;

public class ContInstrumentationPluginConfiguration {

    private String jdkLibsDirectory;
    private boolean debugMode;

    public ContInstrumentationPluginConfiguration() {
        jdkLibsDirectory = System.getProperty("java.home") + "/lib";
    }

    public String getJdkLibsDirectory() {
        return jdkLibsDirectory;
    }

    public void setJdkLibsDirectory(String jdkLibsDirectory) {
        Validate.notEmpty(jdkLibsDirectory);
        this.jdkLibsDirectory = jdkLibsDirectory;
    }

    public boolean isDebugMode() {
        return debugMode;
    }

    public void setDebugMode(boolean debugMode) {
        this.debugMode = debugMode;
    }
}
