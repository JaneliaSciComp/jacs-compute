package org.janelia.jacs2.asyncservice.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.glassfish.jersey.media.multipart.BodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.MultiPartMediaTypes;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.slf4j.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;

public class LSFPACHelper {

    private static class LSFToken {
        public String token;

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }

    public static class LSFJobs {
        @JsonProperty("@total")
        public String total;
        @JsonProperty("Job")
        public List<LSFJob> lsfJobs;

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }

    public static class LSFJob {
        public String id;
        public String user;
        public String cmd;
        public String exitCode;
        public String status;

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }

    private final String lsfPacUrl;
    private final String lsfUser;
    private final String lsfPassword;
    private final Logger logger;
    private final ObjectMapper objectMapper;

    public LSFPACHelper(String lsfPacUrl, String lsfUser, String lsfPassword, Logger logger) {
        this.lsfPacUrl = lsfPacUrl;
        this.lsfUser = lsfUser;
        this.lsfPassword = lsfPassword;
        this.logger = logger;
        this.objectMapper = ObjectMapperFactory.instance().newObjectMapper().configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
    }

    private String login() {
        String apiEndpoint = "/platform/webservice/logon/";
        Client httpclient = null;
        try {
            httpclient = createHttpClient();
            WebTarget target = createTarget(httpclient, apiEndpoint);

            Response response = target.request(MediaType.APPLICATION_JSON)
                    .post(Entity.entity(new StringBuilder()
                                    .append("<User>")
                                    .append("<name>")
                                    .append(lsfUser)
                                    .append("</name>")
                                    .append("<pass>")
                                    .append(lsfPassword)
                                    .append("</pass>")
                                    .append("</User>")
                                    .toString(),
                            MediaType.APPLICATION_XML));
            String entityBody = response.readEntity(String.class);
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                String errmessage;
                if (StringUtils.isNotBlank(entityBody)) {
                    errmessage = objectMapper.readTree(entityBody).get("message").asText();
                } else {
                    errmessage = response.getStatusInfo().getReasonPhrase();
                }
                throw new IllegalStateException(apiEndpoint + " returned with " + response.getStatus() + ": " + errmessage);
            }
            LSFToken token = objectMapper.readValue(entityBody, LSFToken.class);
            return token.token;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpclient != null) {
                httpclient.close();
            }
        }
    }

    private void logout(String authToken) {
        String apiEndpoint = "/platform/ws/logout";
        Client httpclient = null;
        try {
            httpclient = createHttpClient();
            WebTarget target = createTarget(httpclient, apiEndpoint);

            Response response = setRequestHeaders(target.request(MediaType.APPLICATION_JSON), authToken)
                    .post(Entity.text(""));

            String entityBody = response.readEntity(String.class);
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                String errmessage;
                if (StringUtils.isNotBlank(entityBody)) {
                    errmessage = objectMapper.readTree(entityBody).get("message").asText();
                } else {
                    errmessage = response.getStatusInfo().getReasonPhrase();
                }
                throw new IllegalStateException(apiEndpoint + " returned with " + response.getStatus() + ": " + errmessage);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpclient != null) {
                httpclient.close();
            }
        }
    }

    /**
     * Get job(s) info.
     * @param jobId is an LSF job ID. This can be an ID of an array job so the result can be for a list of jobs actually
     *              that are all part of the same array job.
     * @return
     */
    public LSFJobs getJobInfo(String jobId) {
        String apiEndpoint = "/platform/ws/jobs/fullinfo";
        String authToken = login();
        Client httpclient = null;
        try {
            httpclient = createHttpClient();
            WebTarget target = createTarget(httpclient, apiEndpoint)
                    .queryParam("id", jobId)
                    .queryParam("user", lsfUser)
                    ;

            Response response = setRequestHeaders(target.request(MediaType.APPLICATION_JSON), authToken)
                        .get();
            String entityBody = response.readEntity(String.class);
            String errmessage = null;
            if (StringUtils.isNotBlank(entityBody)) {
                JsonNode errMessageNode = objectMapper.readTree(entityBody).get("message");
                if (errMessageNode == null) {
                    errMessageNode = objectMapper.readTree(entityBody).get("errMsg");
                }
                if (errMessageNode != null) {
                    errmessage = errMessageNode.asText();
                }
            }
            if (response.getStatus() != Response.Status.OK.getStatusCode() || StringUtils.isNotBlank(errmessage)) {
                if (StringUtils.isBlank(errmessage)) {
                    errmessage = response.getStatusInfo().getReasonPhrase();
                }
                throw new IllegalStateException(apiEndpoint + " returned with " + response.getStatus() + ": " + errmessage);
            }
            return objectMapper.readValue(entityBody, LSFJobs.class);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpclient != null) {
                httpclient.close();
            }
            logout(authToken);
        }
    }

    public String submitJob(String remoteCommand, String workingDirectory, String outputFile, String errorFile,
                            String nativeSpec) {
        String apiEndpoint = "/platform/ws/jobs/submit";
        String authToken = login();
        Client httpclient = null;
        try {
            httpclient = createHttpClient();
            StringBuilder extraParamsBuilder = new StringBuilder();
            if (StringUtils.isNotBlank(nativeSpec)) {
                extraParamsBuilder.append(nativeSpec)
                    .append(' ');
            }
            extraParamsBuilder.append("-cwd ").append(workingDirectory);

            String appDataFormat = "<AppParam><id>%s</id><value>%s</value><type></type></AppParam>";
            MultiPart dataField = new MultiPart(MultiPartMediaTypes.MULTIPART_MIXED_TYPE)
                    .bodyPart(
                            new BodyPart(MediaType.APPLICATION_XML_TYPE)
                                    .contentDisposition(FormDataContentDisposition.name("COMMANDTORUN").build())
                                    .entity(String.format(appDataFormat, "COMMANDTORUN", remoteCommand)))
                    .bodyPart(
                            new BodyPart(MediaType.APPLICATION_XML_TYPE)
                                    .contentDisposition(FormDataContentDisposition.name("OUTPUT_FILE").build())
                                    .entity(String.format(appDataFormat, "OUTPUT_FILE", outputFile)))
                    .bodyPart(
                            new BodyPart(MediaType.APPLICATION_XML_TYPE)
                                    .contentDisposition(FormDataContentDisposition.name("ERROR_FILE").build())
                                    .entity(String.format(appDataFormat, "ERROR_FILE", errorFile)))
                    .bodyPart(
                            new BodyPart(MediaType.APPLICATION_XML_TYPE)
                                    .contentDisposition(FormDataContentDisposition.name("EXTRA_PARAMS").build())
                                    .entity(String.format(appDataFormat, "EXTRA_PARAMS", extraParamsBuilder.toString())))
                    ;

            MultiPart submitForm = new MultiPart(MultiPartMediaTypes.MULTIPART_MIXED_TYPE)
                    .bodyPart(
                            new BodyPart()
                                    .contentDisposition(FormDataContentDisposition.name("AppName").build())
                                    .entity("generic")
                    )
                    .bodyPart(
                            new BodyPart(MultiPartMediaTypes.MULTIPART_MIXED_TYPE)
                                    .contentDisposition(FormDataContentDisposition.name("data").build())
                                    .entity(dataField)
                    )
                    ;

            WebTarget target = createTarget(httpclient, apiEndpoint);

            Response response = setRequestHeaders(target.request(MediaType.APPLICATION_JSON), authToken)
                    .post(Entity.entity(submitForm, submitForm.getMediaType()))
                    ;

            String entityBody = response.readEntity(String.class);
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                String errmessage;
                if (StringUtils.isNotBlank(entityBody)) {
                    errmessage = objectMapper.readTree(entityBody).get("message").asText();
                } else {
                    errmessage = response.getStatusInfo().getReasonPhrase();
                }
                throw new IllegalStateException(apiEndpoint + " returned with " + response.getStatus() + ": " + errmessage);
            }
            LSFJob submittedJob = objectMapper.readValue(entityBody, LSFJob.class);
            return submittedJob.id;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpclient != null) {
                httpclient.close();
            }
            logout(authToken);
        }
    }

    public void killJob(String jobId) {
        String apiEndpoint = "/platform/ws/jobs/kill";
        String authToken = login();
        Client httpclient = null;
        try {
            httpclient = createHttpClient();

            Form submitForm = new Form()
                    .param("ids", jobId)
                    .param("username", lsfUser);

            WebTarget target = createTarget(httpclient, apiEndpoint);

            Response response = setRequestHeaders(target.request(MediaType.APPLICATION_JSON), authToken)
                    .post(Entity.form(submitForm))
                    ;

            String entityBody = response.readEntity(String.class);
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                String errmessage;
                if (StringUtils.isNotBlank(entityBody)) {
                    errmessage = objectMapper.readTree(entityBody).get("message").asText();
                } else {
                    errmessage = response.getStatusInfo().getReasonPhrase();
                }
                throw new IllegalStateException(apiEndpoint + " returned with " + response.getStatus() + ": " + errmessage);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpclient != null) {
                httpclient.close();
            }
            logout(authToken);
        }
    }

    private Client createHttpClient() throws Exception {
        SSLContext sslContext = SSLContext.getInstance("TLSv1");
        TrustManager[] trustManagers = {
                new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(X509Certificate[] x509Certificates, String authType) throws CertificateException {
                        // Everyone is trusted
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] x509Certificates, String authType) throws CertificateException {
                        // Everyone is trusted
                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                }
        };
        sslContext.init(null, trustManagers, new SecureRandom());

        return ClientBuilder.newBuilder()
                .sslContext(sslContext)
                .hostnameVerifier((s, sslSession) -> true)
                .register(MultiPartFeature.class)
                .build();
    }

    private WebTarget createTarget(Client httpClient, String path) {
        return httpClient.target(lsfPacUrl).path(path);
    }

    private Invocation.Builder setRequestHeaders(Invocation.Builder requestBuilder, String authToken) {
        if (StringUtils.isNotBlank(authToken)) {
            String platformToken = authToken.replace("\"", "#quote#");
            requestBuilder.header("platform_token", platformToken);
            requestBuilder.header("Cookie", "platform_token=" + platformToken);
        }
        return requestBuilder;
    }

}
