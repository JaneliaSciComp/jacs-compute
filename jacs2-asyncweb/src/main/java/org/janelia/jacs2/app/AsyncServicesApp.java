package org.janelia.jacs2.app;

import com.beust.jcommander.JCommander;
import io.undertow.servlet.api.ListenerInfo;
import org.janelia.jacs2.cdi.SeContainerFactory;
import org.janelia.jacs2.job.BackgroundJobs;

import javax.enterprise.inject.se.SeContainer;
import javax.servlet.ServletException;

import static io.undertow.servlet.Servlets.listener;

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
        SeContainer container = SeContainerFactory.getSeContainer();
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
