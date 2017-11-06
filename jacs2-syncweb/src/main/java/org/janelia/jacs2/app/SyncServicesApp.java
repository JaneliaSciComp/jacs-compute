package org.janelia.jacs2.app;

import io.undertow.servlet.api.ListenerInfo;
import org.jboss.weld.environment.servlet.Listener;

import javax.servlet.ServletException;

import static io.undertow.servlet.Servlets.listener;

/**
 * This is the bootstrap application for synchronous services.
 */
public class SyncServicesApp extends AbstractServicesApp {

    public static void main(String[] args) throws ServletException {
        final SyncServicesApp app = new SyncServicesApp();
        app.start(args);
    }

    @Override
    protected String getV2ConfigName() {
        return JAXSyncAppConfig.class.getName();
    }

    @Override
    protected ListenerInfo[] getListeners() {
        return new ListenerInfo[] {
                listener(Listener.class)
        };
    }
}
