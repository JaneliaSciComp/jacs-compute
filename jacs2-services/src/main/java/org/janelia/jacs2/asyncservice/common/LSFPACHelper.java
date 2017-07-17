package org.janelia.jacs2.asyncservice.common;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.HttpEntity;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.FormBodyPart;
import org.apache.http.entity.mime.FormBodyPartBuilder;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MIME;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ByteArrayBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.net.ssl.SSLContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class LSFPACHelper {

    private static class SessionToken {
        String platformToken;
        String sessionId;
    }

    private final String lsfPacUrl;
    private final String lsfUser;
    private final String lsfPassword;
    private final Logger logger;

    public LSFPACHelper(String lsfPacUrl, String lsfUser, String lsfPassword, Logger logger) {
        this.lsfPacUrl = lsfPacUrl;
        this.lsfUser = lsfUser;
        this.lsfPassword = lsfPassword;
        this.logger = logger;
    }

    private String login() {
        String authenticationEndpoint = lsfPacUrl + "/platform/ws/logon";
        CloseableHttpClient httpclient = null;
        try {
            httpclient = getHttpClient();
            HttpPost httpPost = new HttpPost(authenticationEndpoint);

            httpPost.setEntity(EntityBuilder
                            .create()
                            .setText(new StringBuilder()
                                    .append("<User>")
                                    .append("<name>")
                                    .append(lsfUser)
                                    .append("</name>")
                                    .append("<pass>")
                                    .append(lsfPassword)
                                    .append("</pass>")
                                    .append("</User>")
                                    .toString())
                            .setContentType(ContentType.APPLICATION_XML)
                            .build()
            );
            setRequestHeaders(httpPost, null);
            CloseableHttpResponse response = httpclient.execute(httpPost);
            System.out.println(Arrays.asList(response.getAllHeaders()));
            HttpEntity resEntity = response.getEntity();
            Document doc = parseResultEntity(resEntity);
            System.out.println(doc.getElementsByTagName("token").item(0).getTextContent().trim());
            return doc.getElementsByTagName("token").item(0).getTextContent().trim();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpclient != null) {
                try {
                    httpclient.close();
                } catch (IOException ignore) {
                    logger.debug("Error closing the http client", ignore);
                }
            }
        }
    }

    private void logout(String authToken) {
        String authenticationEndpoint = lsfPacUrl + "/platform/ws/logout";
        CloseableHttpClient httpclient = null;
        try {
            httpclient = getHttpClient();
            HttpPost httpRequest = new HttpPost(authenticationEndpoint);
            setRequestHeaders(httpRequest, authToken);
            CloseableHttpResponse response = httpclient.execute(httpRequest);
            System.out.println("Status " + response.getStatusLine());
            HttpEntity resEntity = response.getEntity();
            System.out.println("LOGOUT " + EntityUtils.toString(resEntity));
        } catch (Exception e) {
            throw new IllegalStateException(e);
       } finally {
            if (httpclient != null) {
                try {
                    httpclient.close();
                } catch (IOException ignore) {
                    logger.debug("Error closing the http client", ignore);
                }
            }
        }
    }

    public String getJobStatus(String jobId) {
        String jobInfoEndpoint = lsfPacUrl + "/platform/ws/jobs/" + jobId;
        String authToken = login();
        CloseableHttpClient httpclient = null;
        try {
            httpclient = getHttpClient();
            HttpGet httpRequest = new HttpGet(jobInfoEndpoint);
            setRequestHeaders(httpRequest, authToken);
            CloseableHttpResponse response = httpclient.execute(httpRequest);
            System.out.println("Status " + response.getStatusLine());
            System.out.println(Arrays.asList(response.getAllHeaders()));
            HttpEntity resEntity = response.getEntity();
            return EntityUtils.toString(resEntity);
//            Document resDoc = parseResultEntity(resEntity);
//            XPath xpath = XPathFactory.newInstance().newXPath();
//            XPathExpression expr = xpath.compile("/Jobs/Job/status/text()");
//            return (String) expr.evaluate(resDoc, XPathConstants.STRING);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpclient != null) {
                try {
                    httpclient.close();
                } catch (IOException ignore) {
                    logger.debug("Error closing the http client", ignore);
                }
            }
            logout(authToken);
        }
    }

    public String submitJob(String remoteCommand, String workingDirectory, String outputFile, String errorFile,
                            String nativeSpec) {
        String submitJobEndpoint = lsfPacUrl + "/platform/ws/jobs/submit";
        String authToken = login();
        CloseableHttpClient httpclient = null;
        try {
            httpclient = getHttpClient();
            HttpPost httpRequest = new HttpPost(submitJobEndpoint);
            setRequestHeaders(httpRequest, authToken);
            String appDataFormat = "<AppParam><id>%s</id><value>%s</value><type></type></AppParam>";
            MultipartEntityBuilder dataBuilder = MultipartEntityBuilder.create()
                    .setMode(HttpMultipartMode.BROWSER_COMPATIBLE)
                    .addTextBody("COMMANDTORUN", String.format(appDataFormat, "COMMANDTORUN", remoteCommand), ContentType.APPLICATION_XML)
                    .addTextBody("OUTPUT_FILE", String.format(appDataFormat, "OUTPUT_FILE", outputFile), ContentType.APPLICATION_XML)
                    .addTextBody("ERROR_FILE", String.format(appDataFormat, "ERROR_FILE", errorFile), ContentType.APPLICATION_XML)
                    .addTextBody("EXTRA_PARAMS", String.format(appDataFormat, "EXTRA_PARAMS", nativeSpec), ContentType.APPLICATION_XML)
                    ;

            MultipartEntityBuilder reqEntityBuilder = MultipartEntityBuilder.create()
                    .setMode(HttpMultipartMode.BROWSER_COMPATIBLE)
                    .addPart("AppName", new StringBody(String.valueOf("generic"), ContentType.TEXT_PLAIN))
                    .addTextBody("data", dataBuilder.build().toString(), ContentType.MULTIPART_FORM_DATA)
                    ;
            httpRequest.setEntity(reqEntityBuilder.build());
            System.out.println("Submitted request " + EntityUtils.toString(httpRequest.getEntity()) + " to " + submitJobEndpoint);
            CloseableHttpResponse response = httpclient.execute(httpRequest);
            System.out.println("Status " + response.getStatusLine());
            HttpEntity resEntity = response.getEntity();
            String job = EntityUtils.toString(resEntity);
            System.out.println("SUBMITTED " + job);
            return job;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpclient != null) {
                try {
                    httpclient.close();
                } catch (IOException ignore) {
                    logger.debug("Error closing the http client", ignore);
                }
            }
        }
    }

    public void killJob(String jobId) {
        String killJobEndpoint = lsfPacUrl + "/platform/ws/jobs/kill";
        String authToken = login();
        CloseableHttpClient httpclient = null;
        try {
            httpclient = getHttpClient();
            HttpPost httpRequest = new HttpPost(killJobEndpoint);
            setRequestHeaders(httpRequest, authToken);
            httpRequest.setEntity(EntityBuilder
                            .create()
                            .setText(new StringBuilder()
                                    .append("ids=").append(jobId)
                                    .toString())
                            .setContentType(ContentType.APPLICATION_FORM_URLENCODED)
                            .build()
            );
            CloseableHttpResponse response = httpclient.execute(httpRequest);
            HttpEntity resEntity = response.getEntity();
            EntityUtils.consumeQuietly(resEntity);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpclient != null) {
                try {
                    httpclient.close();
                } catch (IOException ignore) {
                    logger.debug("Error closing the http client", ignore);
                }
            }
        }
    }

    private CloseableHttpClient getHttpClient() throws Exception {
        SSLContext sslContext = new SSLContextBuilder()
                .loadTrustMaterial(null, (certificate, authType) -> true).build();

        CloseableHttpClient httpClient = HttpClients.custom()
                .setSSLContext(sslContext)
                .setSSLHostnameVerifier(new NoopHostnameVerifier())
                .build();

        return httpClient;
    }

    private Document parseResultEntity(HttpEntity resEntity) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        DocumentBuilder builder = factory.newDocumentBuilder();
        return builder.parse(resEntity.getContent());
    }

    private void setRequestHeaders(HttpRequestBase httpRequest, String authToken) {
        httpRequest.addHeader("Accept", "application/xml");
        httpRequest.addHeader("Content-Type", "application/xml");
        if (StringUtils.isNotBlank(authToken)) {
            String platformToken = authToken.replace("\"", "#quote#");
            System.out.println("PLATFORM TOKEN:" + platformToken);
            httpRequest.addHeader("platform_token", platformToken);
            httpRequest.addHeader("Cookie", "platform_token=" + platformToken);
            httpRequest.addHeader("REMOTE_USER", lsfUser);
        }
    }

    public static void main(String[] args) {
        String jobId = "87718%5B1%5D";
        LSFPACHelper lsfPacHelper = new LSFPACHelper("https://lsf-pac:8443", args[0], args[1], LoggerFactory.getLogger(LSFPACHelper.class));
//        String status = lsfPacHelper.getJobStatus(jobId);
//        System.out.println("Job " + jobId + " status " + status);
        String newJob = lsfPacHelper.submitJob(
                "/home/goinac/Work/jacs-2/local/test/testCmd.sh 'display this' /home/goinac/Work/jacs-2/local/test/flylightSample.20x.sh",
                "/home/goinac/Work/jacs-2/tt/tempWorkingDir",
                "/home/goinac/Work/jacs-2/tt/tempWorkingDir/test.out",
                "/home/goinac/Work/jacs-2/tt/tempWorkingDir/test.err",
                "-P jacs -n 1 -R \"select[haswell]\"");
        System.out.println("New Job " + newJob);
    }
}
