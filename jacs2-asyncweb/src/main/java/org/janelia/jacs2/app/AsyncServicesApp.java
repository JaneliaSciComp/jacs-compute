package org.janelia.jacs2.app;

import static io.undertow.servlet.Servlets.listener;
import io.undertow.servlet.api.ListenerInfo;
import org.janelia.jacs2.job.BackgroundJobs;
import org.jboss.weld.environment.servlet.Listener;

import javax.servlet.ServletException;

/**
 * This is the bootstrap application for asynchronous services.
 */
public class AsyncServicesApp extends AbstractServicesApp {

    public static void main(String[] args) throws ServletException {
        final AsyncServicesApp app = new AsyncServicesApp();
        app.start(args);
    }

    @Override
    protected String getJaxConfigName() {
        return JAXAsyncAppConfig.class.getName();
    }

    @Override
    protected String getRestApiMapping() {
        return "/jacs-api/*";
    }

    @Override
    protected ListenerInfo[] getListeners() {
        return new ListenerInfo[] {
                listener(Listener.class),
                listener(BackgroundJobs.class)
        };
    }

}
