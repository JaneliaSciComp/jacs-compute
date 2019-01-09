package org.janelia.jacs2.app;

import com.beust.jcommander.JCommander;
import io.swagger.jersey.config.JerseyJaxrsConfig;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.handlers.PathHandler;
import io.undertow.server.handlers.resource.PathResourceManager;
import io.undertow.server.handlers.resource.ResourceHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.ListenerInfo;
import io.undertow.servlet.api.ServletInfo;
import org.glassfish.jersey.servlet.ServletContainer;
import org.janelia.jacs2.app.undertow.UndertowContainerInitializer;
import org.janelia.jacs2.cdi.ApplicationConfigProvider;
import org.jboss.weld.environment.servlet.Listener;
import org.jboss.weld.module.web.servlet.WeldInitialListener;
import org.jboss.weld.module.web.servlet.WeldTerminalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContextListener;
import javax.servlet.ServletException;
import javax.ws.rs.core.Application;
import java.nio.file.Paths;
import java.util.EventListener;
import java.util.List;

import static io.undertow.Handlers.resource;
import static io.undertow.servlet.Servlets.servlet;

/**
 * This is the bootstrap application for JACS services.
 */
public abstract class AbstractServicesApp {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractServicesApp.class);

    private Undertow server;

    static <A extends AppArgs> A parseAppArgs(String[] args, A appArgs) {
        JCommander cmdline = new JCommander(appArgs);
        cmdline.parse(args);
        // update the dynamic config
        ApplicationConfigProvider.setAppDynamicArgs(appArgs.appDynamicConfig);
        return appArgs;
    }

    static <A extends AppArgs> void displayAppUsage(A appArgs) {
        displayAppUsage(appArgs, new StringBuilder());
    }

    static <A extends AppArgs> void displayAppUsage(A appArgs, StringBuilder output) {
        JCommander cmdline = new JCommander(appArgs);
        cmdline.usage(output);
    }

    protected void start(AppArgs appArgs) {
        ContainerInitializer containerInitializer = new UndertowContainerInitializer(
                getApplicationId(),
                getRestApiContext(),
                getApiVersion(),
                getPathsExcludedFromAccessLog(),
                getAppListeners()
        );
        try {
            containerInitializer.initialize(this.getJaxApplication(), appArgs);
            containerInitializer.start();
        } catch (Exception e) {
            LOG.error("Error starting the application", e);
        }
    }

    String getApiVersion() {
        return "v2";
    }

    abstract String getApplicationId();

    abstract Application getJaxApplication();

    String getRestApiContext() {
        return "/api";
    }

    String[] getPathsExcludedFromAccessLog() {
        return new String[0];
    }

    abstract List<Class<? extends EventListener>> getAppListeners();

}
