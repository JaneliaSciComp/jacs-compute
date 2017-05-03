package org.janelia.jacs2.app;

import static io.undertow.servlet.Servlets.listener;
import static io.undertow.servlet.Servlets.servlet;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.handlers.PathHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.FilterInfo;
import io.undertow.servlet.api.ServletInfo;
import org.glassfish.jersey.servlet.ServletContainer;
import org.janelia.jacs2.job.BackgroundJobs;
import org.jboss.weld.environment.servlet.Listener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;
import javax.servlet.ServletException;

/**
 * This is the bootstrap application for asynchronous services.
 */
public class AsyncServicesApp {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncServicesApp.class);

    private Undertow server;

    private static class AppArgs {
        @Parameter(names = "-b", description = "Binding IP", required = false)
        private String host = "localhost";
        @Parameter(names = "-p", description = "Listner port number", required = false)
        private int portNumber = 9080;
        @Parameter(names = "-name", description = "Deployment name", required = false)
        private String deployment = "jacs-async";
        @Parameter(names = "-context-path", description = "Base context path", required = false)
        private String baseContextPath = "jacs-async";
        @Parameter(names = "-h", description = "Display help", arity = 0, required = false)
        private boolean displayUsage = false;
    }

    public static void main(String[] args) throws ServletException {
        final AppArgs appArgs = new AppArgs();
        JCommander cmdline = new JCommander(appArgs, args);
        if (appArgs.displayUsage) {
            cmdline.usage();
        } else {
            AsyncServicesApp app = createApp(appArgs);
            app.run();
        }
    }

    private static AsyncServicesApp createApp(AppArgs appArgs) throws ServletException {
        AsyncServicesApp app = new AsyncServicesApp();

        String contextPath = "/" + appArgs.baseContextPath;

        ServletInfo restApiServlet =
            servlet("restApiServlet", ServletContainer.class)
                .setLoadOnStartup(1)
                .addInitParam("javax.ws.rs.Application", JAXAsyncAppConfig.class.getName())
                .addMapping("/jacs-api/*");

        DeploymentInfo servletBuilder =
            Servlets.deployment()
                    .setClassLoader(AsyncServicesApp.class.getClassLoader())
                    .setContextPath(contextPath)
                    .setDeploymentName(appArgs.deployment)
                    .addFilter(new FilterInfo("corsFilter", CORSResponseFilter.class))
                    .addFilterUrlMapping("corsFilter", "/*", DispatcherType.REQUEST)
                    .addListeners(
                            listener(Listener.class),
                            listener(BackgroundJobs.class)
                    )
                    .addServlets(restApiServlet);

        DeploymentManager deploymentManager = Servlets.defaultContainer().addDeployment(servletBuilder);
        deploymentManager.deploy();

        PathHandler jacs2Handler =
            Handlers.path(Handlers.redirect(contextPath))
                .addPrefixPath(contextPath, deploymentManager.start());

        LOG.info("Start JACSv2 listener on {}:{}", appArgs.host, appArgs.portNumber);
        app.server = Undertow
                        .builder()
                        .addHttpListener(appArgs.portNumber, appArgs.host)
                        .setHandler(jacs2Handler)
                        .build();
        return app;
    }

    private void run() {
        server.start();
    }

}
