package org.janelia.jacs2.app;

import org.janelia.jacs2.cdi.SeContainerFactory;
import org.janelia.jacs2.job.BackgroundJobs;

import javax.enterprise.inject.se.SeContainer;
import javax.servlet.ServletException;
import javax.ws.rs.core.Application;
import java.util.Collections;
import java.util.EventListener;
import java.util.List;

/**
 * This is the bootstrap application for asynchronous services.
 */
public class AsyncServicesApp extends AbstractServicesApp {

    private static final String DEFAULT_APP_ID = "JacsAsyncServices";

    public static void main(String[] args) {
        final AppArgs appArgs = parseAppArgs(args, new AppArgs());
        if (appArgs.displayUsage) {
            displayAppUsage(appArgs);
            return;
        }
        SeContainer container = SeContainerFactory.getSeContainer();
        AsyncServicesApp app = container.select(AsyncServicesApp.class).get();
        app.start(appArgs);
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
