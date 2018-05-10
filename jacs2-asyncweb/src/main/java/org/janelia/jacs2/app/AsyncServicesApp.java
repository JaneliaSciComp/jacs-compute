package org.janelia.jacs2.app;

import static io.undertow.servlet.Servlets.listener;

import com.beust.jcommander.JCommander;
import io.undertow.servlet.api.ListenerInfo;
import org.janelia.jacs2.job.BackgroundJobs;
import org.jboss.weld.environment.servlet.Listener;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.servlet.ServletException;

/**
 * This is the bootstrap application for asynchronous services.
 */
public class AsyncServicesApp extends AbstractServicesApp {

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
        AsyncServicesApp app = container.select(AsyncServicesApp.class).get();
        app.start(appArgs);
    }

    @Override
    protected String getV2ConfigName() {
        return JAXAsyncAppConfig.class.getName();
    }

    @Override
    protected ListenerInfo[] getAppListeners() {
        return new ListenerInfo[] {
                listener(BackgroundJobs.class)
        };
    }

}
