package org.janelia.jacs2.cdi;

import org.janelia.jacs2.asyncservice.sample.SingularityApp;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Provider;

/**
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@ApplicationScoped
public class SingularityAppFactory {

    @Inject
    private Provider<SingularityApp> appProvider;

    public SingularityApp getSingularityApp(String containerName, String appName) {
        SingularityApp singularityApp = appProvider.get();
        singularityApp.setContainerName(containerName);
        singularityApp.setAppName(appName);
        singularityApp.resolve();
        return singularityApp;
    }

    public SingularityApp getSingularityApp(String containerName, String containerVersion, String appName) {
        SingularityApp singularityApp = appProvider.get();
        singularityApp.setContainerName(containerName);
        singularityApp.setContainerVersion(containerVersion);
        singularityApp.setAppName(appName);
        singularityApp.resolve();
        return singularityApp;
    }

}
