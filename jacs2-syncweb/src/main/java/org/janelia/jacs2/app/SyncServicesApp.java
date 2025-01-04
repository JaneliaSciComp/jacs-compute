package org.janelia.jacs2.app;

import java.util.Collections;
import java.util.EventListener;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;
import jakarta.ws.rs.core.Application;

import org.janelia.jacs2.cdi.SeContainerFactory;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.config.ApplicationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the bootstrap application for synchronous services.
 */
@ApplicationScoped
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
            ApplicationConfig appConfig = container.select(ApplicationConfig.class, new ApplicationProperties() {
                @Override
                public Class<ApplicationProperties> annotationType() {
                    return ApplicationProperties.class;
                }
            }).get();

            LOG.info("Start application with {}", appConfig);
            app.start(appArgs, appConfig);
        } catch (Throwable e) {
            // For some reason, any Throwables thrown out of this main function are discarded. Thus, we must log them
            // here. Of course, this will be problematic if there is ever an issue with the logger.
            LOG.error("Error starting application", e);
            e.printStackTrace(System.err);
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
