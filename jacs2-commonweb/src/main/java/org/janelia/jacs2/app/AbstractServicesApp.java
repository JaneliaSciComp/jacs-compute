package org.janelia.jacs2.app;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.handlers.PathHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.FilterInfo;
import io.undertow.servlet.api.ListenerInfo;
import io.undertow.servlet.api.ServletInfo;
import org.glassfish.jersey.servlet.ServletContainer;
import org.janelia.jacs2.cdi.ApplicationPropertiesProvider;
import org.janelia.jacs2.filter.CORSResponseFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;
import javax.servlet.ServletException;

import java.util.Map;

import static io.undertow.servlet.Servlets.servlet;

/**
 * This is the bootstrap application for JACS services.
 */
public abstract class AbstractServicesApp {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractServicesApp.class);

    private Undertow server;

    protected static class AppArgs {
        @Parameter(names = "-b", description = "Binding IP", required = false)
        protected String host = "localhost";
        @Parameter(names = "-p", description = "Listner port number", required = false)
        protected int portNumber = 8080;
        @Parameter(names = "-name", description = "Deployment name", required = false)
        protected String deployment = "jacs";
        @Parameter(names = "-context-path", description = "Base context path", required = false)
        protected String baseContextPath = "jacs";
        @Parameter(names = "-h", description = "Display help", arity = 0, required = false)
        private boolean displayUsage = false;
        @DynamicParameter(names = "-D", description = "Dynamic application parameters that could override application properties")
        private Map<String, String> applicationArgs = ApplicationPropertiesProvider.applicationArgs();
    }

    protected void start(String[] args) throws ServletException {
        final AppArgs appArgs = new AppArgs();
        JCommander cmdline = new JCommander(appArgs, args);
        if (appArgs.displayUsage) {
            cmdline.usage();
        } else {
            initializeApp(appArgs);
            run();
        }
    }

    protected void initializeApp(AppArgs appArgs) throws ServletException {
        String contextPath = "/" + appArgs.baseContextPath;

        ServletInfo restApiServlet =
                servlet("restApiServlet", ServletContainer.class)
                        .setLoadOnStartup(1)
                        .addInitParam("javax.ws.rs.Application", getJaxConfigName())
                        .addMapping(getRestApiMapping());

        DeploymentInfo servletBuilder =
                Servlets.deployment()
                        .setClassLoader(this.getClass().getClassLoader())
                        .setContextPath(contextPath)
                        .setDeploymentName(appArgs.deployment)
                        .addFilter(new FilterInfo("corsFilter", CORSResponseFilter.class))
                        .addFilterUrlMapping("corsFilter", "/*", DispatcherType.REQUEST)
                        .addListeners(getListeners())
                        .addServlets(restApiServlet);

        DeploymentManager deploymentManager = Servlets.defaultContainer().addDeployment(servletBuilder);
        deploymentManager.deploy();

        PathHandler jacs2Handler =
                Handlers.path(Handlers.redirect(contextPath))
                        .addPrefixPath(contextPath, deploymentManager.start());

        LOG.info("Start JACSv2 listener on {}:{}", appArgs.host, appArgs.portNumber);
        server = Undertow
                    .builder()
                    .addHttpListener(appArgs.portNumber, appArgs.host)
                    .setHandler(jacs2Handler)
                    .build();

    }

    private void run() {
        server.start();
    }

    protected abstract String getJaxConfigName();

    protected abstract String getRestApiMapping();

    protected abstract ListenerInfo[] getListeners();
}
