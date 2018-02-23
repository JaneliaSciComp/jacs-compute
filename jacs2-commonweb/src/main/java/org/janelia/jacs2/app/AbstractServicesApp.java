package org.janelia.jacs2.app;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.swagger.jersey.config.JerseyJaxrsConfig;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.handlers.PathHandler;
import io.undertow.server.handlers.resource.PathResourceManager;
import io.undertow.server.handlers.resource.ResourceHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.*;
import org.glassfish.jersey.servlet.ServletContainer;
import org.janelia.jacs2.cdi.ApplicationConfigProvider;
import org.janelia.jacs2.filter.AuthFilter;
import org.janelia.jacs2.filter.CORSResponseFilter;
import org.jboss.weld.environment.servlet.Listener;
import org.jboss.weld.module.web.servlet.WeldInitialListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;
import javax.servlet.ServletException;
import java.nio.file.Paths;
import java.util.Map;

import static io.undertow.Handlers.resource;
import static io.undertow.servlet.Servlets.servlet;

/**
 * This is the bootstrap application for JACS services.
 */
public abstract class AbstractServicesApp {

    private static final Logger log = LoggerFactory.getLogger(AbstractServicesApp.class);

    private Undertow server;

    protected static class AppArgs {
        @Parameter(names = "-s", description = "Hostname", required = false)
        protected String hostname = "localhost";
        @Parameter(names = "-b", description = "Binding IP", required = false)
        protected String host = "localhost";
        @Parameter(names = "-p", description = "Listener port number", required = false)
        protected int portNumber = 8080;
        @Parameter(names = "-h", description = "Display help", arity = 0, required = false)
        protected boolean displayUsage = false;
        @DynamicParameter(names = "-D", description = "Dynamic application parameters that could override application properties")
        private Map<String, String> applicationArgs = ApplicationConfigProvider.applicationArgs();
    }

    protected void start(AppArgs appArgs) throws ServletException {
        initializeApp(appArgs);
        run();
    }

    protected void initializeApp(AppArgs appArgs) throws ServletException {
        String baseUrl = "http://"+appArgs.hostname+":"+appArgs.portNumber;
        String prefixPathApi = "/api";
        String prefixPathDocs = "/docs";

        // API Version 1 was implemented in JACSv1

        // API Version 2
        DeploymentInfo deployment2 = getVersion2Deployment(prefixPathApi, baseUrl);
        DeploymentManager deploymentManager = Servlets.defaultContainer().addDeployment(deployment2);

        // When adding a new version of the API, make sure to also add it to the Swagger index.html

        // Static handler for Swagger resources
        ResourceHandler staticHandler =
                resource(new PathResourceManager(Paths.get("webapp"), 100));

        // Combine everything into a handler
        deploymentManager.deploy();
        PathHandler jacs2Handler = Handlers.path(
                Handlers.redirect(prefixPathDocs)) // redirect root to /docs
                .addPrefixPath(prefixPathDocs, staticHandler)
                .addPrefixPath(prefixPathApi, deploymentManager.start());

        log.info("Start JACSv2 listener on {}:{}", appArgs.host, appArgs.portNumber);
        server = Undertow
                    .builder()
                    .addHttpListener(appArgs.portNumber, appArgs.host)
                    .setHandler(jacs2Handler)
                    .build();

    }

    private DeploymentInfo getVersion2Deployment(String prefixPath, String baseUrl) {
        String apiVersionToken = "v2";
        String restApiName = "rest-"+apiVersionToken;
        String contextPath = prefixPath+"/"+restApiName;

        ServletInfo restApiServlet =
                servlet("restApiServlet", ServletContainer.class)
                        .setLoadOnStartup(1)
                        .addInitParam("javax.ws.rs.Application", getV2ConfigName())
                        .addMapping("/*");

        // Construct full URL for Swagger invocations
        String basepath = baseUrl+contextPath;

        // No mapping because this servlet as it is only used for initialization and doesn't actually expose any interface.
        ServletInfo swaggerServlet =
                servlet("swaggerServlet", JerseyJaxrsConfig.class)
                        .setLoadOnStartup(2)
                        .addInitParam("api.version", apiVersionToken)
                        .addInitParam("swagger.api.basepath", basepath);

        DeploymentInfo deployment =
                Servlets.deployment()
                        .setClassLoader(this.getClass().getClassLoader())
                        .setContextPath(contextPath)
                        .setDeploymentName(restApiName)
                        .addFilter(new FilterInfo("corsFilter", CORSResponseFilter.class))
                        .addFilterUrlMapping("corsFilter", "/*", DispatcherType.REQUEST)
                        .addListener(Servlets.listener(Listener.class))
                        .addListeners(getAppListeners())
                        .addServlets(restApiServlet, swaggerServlet);

        log.info("Deploying REST API servlet at "+contextPath);

        return deployment;
    }

    private void run() {
        server.start();
    }

    protected abstract String getV2ConfigName();

    protected abstract ListenerInfo[] getAppListeners();
}
