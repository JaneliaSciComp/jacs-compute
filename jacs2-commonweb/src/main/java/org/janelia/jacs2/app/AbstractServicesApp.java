package org.janelia.jacs2.app;

import com.beust.jcommander.JCommander;

import io.undertow.Undertow;
import org.janelia.jacs2.app.undertow.UndertowAppContainer;
import org.janelia.jacs2.cdi.ApplicationConfigProvider;
import org.janelia.jacs2.config.ApplicationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Application;

import java.util.EventListener;
import java.util.List;

import static io.undertow.servlet.Servlets.servlet;

/**
 * This is the bootstrap application for JACS services.
 */
public abstract class AbstractServicesApp {

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

    protected void start(AppArgs appArgs, ApplicationConfig applicationConfig) throws Exception {
        AppContainer appContainer = new UndertowAppContainer(
                getApplicationId(),
                getRestApiContext(),
                getApiVersion(),
                getPathsExcludedFromAccessLog(),
                applicationConfig,
                getAppListeners()
        );
        appContainer.initialize(this.getJaxApplication(), appArgs);
        appContainer.start();
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
