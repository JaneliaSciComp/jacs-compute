package org.janelia.jacs2.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.ws.rs.core.Application;
import java.util.Collections;
import java.util.EventListener;
import java.util.List;

/**
 * This is the bootstrap application for synchronous services.
 */
public class SyncServicesApp extends AbstractServicesApp {

    private static final Logger LOG = LoggerFactory.getLogger(SyncServicesApp.class);
    private static final String DEFAULT_APP_ID = "JacsSyncServices";

    public static void main(String[] args) {
        try {
            final AppArgs appArgs = parseAppArgs(args, new AppArgs());
            if (appArgs.displayUsage) {
                displayAppUsage(appArgs);
                return;
            }
            SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
            SeContainer container = containerInit.initialize();
            SyncServicesApp app = container.select(SyncServicesApp.class).get();
            app.start(appArgs);
        } catch (Throwable e) {
            // For some reason, any Throwables thrown out of this main function are discarded. Thus, we must log them
            // here. Of course, this will be problematic if there is ever an issue with the logger.
            LOG.error("Error starting application", e);
        }
    }

    @Override
    String getApplicationId() {
        return DEFAULT_APP_ID;
    }

    @Override
    Application getJaxApplication() {
        return new JAXSyncAppConfig();
    }

    @Override
    List<Class<? extends EventListener>> getAppListeners() {
        return Collections.emptyList();
    }

}
