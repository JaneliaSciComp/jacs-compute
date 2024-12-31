package org.janelia.jacs2.cdi;

import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;

public class SeContainerFactory {

    private static SeContainerInitializer seContainerInit;
    private static SeContainer seContainer;

    public synchronized static SeContainer getSeContainer() {
        if (seContainerInit == null) seContainerInit = SeContainerInitializer.newInstance().addExtensions(new OnStartupExtension());
        if (seContainer == null) seContainer = seContainerInit.initialize();
        return seContainer;
    }
}
