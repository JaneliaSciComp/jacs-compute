package org.janelia.jacs2.cdi;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;

public class SeContainerFactory {

    private static SeContainerInitializer seContainerInit;
    private static SeContainer seContainer;

    public synchronized static SeContainer getSeContainer() {
        if (seContainerInit == null) seContainerInit = SeContainerInitializer.newInstance();
        if (seContainer == null) seContainer = seContainerInit.initialize();
        return seContainer;
    }
}
