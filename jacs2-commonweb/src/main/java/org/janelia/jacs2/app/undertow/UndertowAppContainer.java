package org.janelia.jacs2.app.undertow;

import java.nio.file.Paths;
import java.util.EventListener;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.ws.rs.core.Application;

import com.google.common.collect.ImmutableList;

import io.swagger.jersey.config.JerseyJaxrsConfig;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.attribute.BytesSentAttribute;
import io.undertow.attribute.ConstantExchangeAttribute;
import io.undertow.attribute.CookieAttribute;
import io.undertow.attribute.DateTimeAttribute;
import io.undertow.attribute.ExchangeAttribute;
import io.undertow.attribute.QueryStringAttribute;
import io.undertow.attribute.RemoteHostAttribute;
import io.undertow.attribute.RemoteUserAttribute;
import io.undertow.attribute.RequestHeaderAttribute;
import io.undertow.attribute.RequestMethodAttribute;
import io.undertow.attribute.ResponseCodeAttribute;
import io.undertow.attribute.ResponseHeaderAttribute;
import io.undertow.predicate.Predicate;
import io.undertow.predicate.Predicates;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.accesslog.AccessLogHandler;
import io.undertow.server.handlers.resource.PathResourceManager;
import io.undertow.server.handlers.resource.ResourceHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.ListenerInfo;
import io.undertow.servlet.api.ServletInfo;
import io.undertow.util.HttpString;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.servlet.ServletContainer;
import org.glassfish.jersey.servlet.ServletProperties;
import org.janelia.jacs2.app.AppArgs;
import org.janelia.jacs2.app.AppContainer;
import org.janelia.jacs2.app.ContextPathBuilder;
import org.janelia.jacs2.config.ApplicationConfig;
import org.jboss.weld.environment.servlet.Listener;
import org.jboss.weld.module.web.servlet.WeldInitialListener;
import org.jboss.weld.module.web.servlet.WeldTerminalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.undertow.Handlers.resource;
import static io.undertow.servlet.Servlets.servlet;

public class UndertowAppContainer implements AppContainer {

    private static final Logger LOG = LoggerFactory.getLogger(UndertowAppContainer.class);

    private final String applicationId;
    private final String restApiContext;
    private final String restApiVersion;
    private final String[] excludedPathsFromAccessLog;
    private final ApplicationConfig applicationConfig;
    private final List<ListenerInfo> appListeners;

    private Undertow server;

    public UndertowAppContainer(String applicationId,
                                String restApiContext,
                                String restApiVersion,
                                String[] excludedPathsFromAccessLog,
                                ApplicationConfig applicationConfig,
                                List<Class<? extends EventListener>> appListenerTypes) {
        this.applicationId = applicationId;
        this.restApiContext = restApiContext;
        this.restApiVersion = restApiVersion;
        this.excludedPathsFromAccessLog = excludedPathsFromAccessLog;
        this.applicationConfig = applicationConfig;
        this.appListeners = appListenerTypes.stream().map(Servlets::listener).collect(Collectors.toList());
    }

    @Override
    public void initialize(Application application, AppArgs appArgs) throws ServletException {
        String deploymentName = "rest-" + restApiVersion;
        String contextPath = new ContextPathBuilder()
                .path(restApiContext)
                .path(deploymentName)
                .build();
        String docsContextPath = "/docs";
        ServletInfo restApiServlet =
                Servlets.servlet("restApiServlet", ServletContainer.class)
                        .setLoadOnStartup(1)
                        .setAsyncSupported(true)
                        .setEnabled(true)
                        .addInitParam(ServletProperties.JAXRS_APPLICATION_CLASS, application.getClass().getName())
                        .addInitParam("jersey.config.server.wadl.disableWadl", "true")
                        .addMappings("/*", "/authenticated/*", "/unauthenticated/*")
                ;

        String basepath = "http://" + appArgs.hostname + ":" + appArgs.portNumber + contextPath;
        ServletInfo swaggerDocsServlet =
                servlet("swaggerDocsServlet", JerseyJaxrsConfig.class)
                        .setLoadOnStartup(2)
                        .addInitParam("api.version", restApiVersion)
                        .addInitParam("swagger.api.basepath", basepath);

        DeploymentInfo servletBuilder =
                Servlets.deployment()
                        .setClassLoader(this.getClass().getClassLoader())
                        .setContextPath(contextPath)
                        .setDeploymentName(deploymentName)
                        .setEagerFilterInit(true)
                        .addListener(Servlets.listener(WeldInitialListener.class))
                        .addListener(Servlets.listener(Listener.class))
                        .addListeners(appListeners)
                        .addListener(Servlets.listener(WeldTerminalListener.class))
                        .addServlets(restApiServlet, swaggerDocsServlet)
                ;

        LOG.info("Deploying REST API at {}", basepath);
        DeploymentManager deploymentManager = Servlets.defaultContainer().addDeployment(servletBuilder);
        deploymentManager.deploy();
        HttpHandler restApiHttpHandler = deploymentManager.start();

        // Static handler for Swagger resources
        ResourceHandler staticHandler =
                resource(new PathResourceManager(Paths.get("swagger-webapp"), 100));

        HttpHandler jacsHandler = new AccessLogHandler(
                Handlers.path(
                        Handlers.redirect(docsContextPath))
                        .addPrefixPath(docsContextPath, staticHandler)
                        .addPrefixPath(contextPath, new SavedRequestBodyHandler(
                                restApiHttpHandler,
                                applicationConfig.getBooleanPropertyValue("AccessLog.WithRequestBody", false),
                                applicationConfig.getStringListPropertyValue("AccessLog.RestrictedPaths",
                                        ImmutableList.of("/auth/authenticate", "/data/user/password")))),
                new Slf4jAccessLogReceiver(LoggerFactory.getLogger(application.getClass())),
                "ignored",
                new JoinedExchangeAttribute(new ExchangeAttribute[] {
                        RemoteHostAttribute.INSTANCE, // <RemoteIP>
                        new AuthenticatedUserAttribute(), // <RemoteUser>
                        new RequestHeaderAttribute(new HttpString("Application-Id")), // <Application-Id>
                        RequestMethodAttribute.INSTANCE, // <HttpVerb>
                        new RequestFullURLAttribute(), // <Request URL>
                        QueryStringAttribute.INSTANCE, // <RequestQuery>
                        new NameValueAttribute("location", new ResponseHeaderAttribute(new HttpString("Location")), true), // location=<ResponseLocation>
                        new NameValueAttribute("status", ResponseCodeAttribute.INSTANCE), // status=<ResponseStatus>
                        new NameValueAttribute("response_bytes", new BytesSentAttribute(false)), // response_bytes=<ResponseBytes>
                        new NameValueAttribute("rt", new ResponseTimeAttribute()), // rt=<ResponseTimeInSeconds>
                        new NameValueAttribute("tp", new ThroughputAttribute()), // tp=<Throughput>
                        new NameValueAttribute("sessionId", new CookieAttribute("JSESSIONID"), true),
                        new NameValueAttribute("requestHeaders", new RequestHeadersAttribute(getOmittedHeaders())),
                        new RequestBodyAttribute(applicationConfig.getIntegerPropertyValue("AccessLog.MaxRequestBody")) // Request Body
                }, " "),
                getAccessLogFilter()
        );

        LOG.info("Starting JACS {}-{} listener on {}:{}", applicationId, restApiVersion, appArgs.host, appArgs.portNumber);
        server = Undertow
                .builder()
                .addHttpListener(appArgs.portNumber, appArgs.host)
                .setIoThreads(appArgs.nIOThreads)
                .setServerOption(UndertowOptions.RECORD_REQUEST_START_TIME, true)
                .setWorkerThreads(appArgs.nWorkers)
                .setHandler(jacsHandler)
                .build();
    }

    @Override
    public void start() {
        server.start();
    }

    private Predicate getAccessLogFilter() {
        return Predicates.not(
                Predicates.prefixes(excludedPathsFromAccessLog)
        );
    }

    private java.util.function.Predicate<HttpString> getOmittedHeaders() {
        Set<HttpString> ignoredHeaders =
                applicationConfig.getStringListPropertyValue("AccessLog.OmittedHeaders").stream()
                        .filter(h -> StringUtils.isNotBlank(h))
                        .map(h -> new HttpString(h.trim()))
                        .collect(Collectors.toSet());
        return h -> ignoredHeaders.contains("*") || ignoredHeaders.contains(h);
    }
}
