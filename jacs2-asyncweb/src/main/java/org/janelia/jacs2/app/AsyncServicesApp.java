package org.janelia.jacs2.app;

import java.util.Collections;
import java.util.EventListener;
import java.util.List;

import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;
import jakarta.ws.rs.core.Application;

import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.job.BackgroundJobs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the bootstrap application for asynchronous services.
 */
public class AsyncServicesApp extends AbstractServicesApp {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncServicesApp.class);
    private static final String DEFAULT_APP_ID = "JacsAsyncServices";

    public static void main(String[] args) {
        try {
            final AppArgs appArgs = parseAppArgs(args, new AppArgs());
            if (appArgs.displayUsage) {
                displayAppUsage(appArgs);
                return;
            }
            SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
            SeContainer container = containerInit.initialize();
            AsyncServicesApp app = container.select(AsyncServicesApp.class).get();
            ApplicationConfig appConfig = container.select(ApplicationConfig.class, new ApplicationProperties() {
                @Override
                public Class<ApplicationProperties> annotationType() {
                    return ApplicationProperties.class;
                }
            }).get();

            app.start(appArgs, appConfig);
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
        return new JAXAsyncAppConfig();
    }

    @Override
    List<Class<? extends EventListener>> getAppListeners() {
        return Collections.singletonList(BackgroundJobs.class);
    }

}
