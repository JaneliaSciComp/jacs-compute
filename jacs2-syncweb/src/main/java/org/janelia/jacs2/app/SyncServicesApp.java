package org.janelia.jacs2.app;

import com.beust.jcommander.JCommander;
import io.undertow.servlet.api.ListenerInfo;
import org.jboss.weld.environment.servlet.Listener;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.servlet.ServletException;

import static io.undertow.servlet.Servlets.listener;

/**
 * This is the bootstrap application for synchronous services.
 */
public class SyncServicesApp extends AbstractServicesApp {

    public static void main(String[] args) throws ServletException {
        final AppArgs appArgs = new AppArgs();
        JCommander cmdline = new JCommander(appArgs);
        cmdline.parse(args);
        if (appArgs.displayUsage) {
            cmdline.usage();
            return;
        }
        SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
        SeContainer container = containerInit.initialize();
        SyncServicesApp app = container.select(SyncServicesApp.class).get();
        app.start(appArgs);
    }

    @Override
    protected String getV2ConfigName() {
        return JAXSyncAppConfig.class.getName();
    }

    @Override
    protected ListenerInfo[] getAppListeners() {
        return new ListenerInfo[] {
        };
    }
}
