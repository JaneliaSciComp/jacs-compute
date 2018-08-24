package org.janelia.jacs2.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.janelia.jacs2.cdi.ObjectMapperFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class HttpUtils {

    public static Client createHttpClient() throws Exception {
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

        ObjectMapper objectMapper = ObjectMapperFactory.instance().newObjectMapper();
        JacksonJaxbJsonProvider jacksonProvider = new JacksonJaxbJsonProvider();
        jacksonProvider.setMapper(objectMapper);
        ClientConfig clientConfig = new ClientConfig()
                .register(jacksonProvider);
        return ClientBuilder.newBuilder()
                .withConfig(clientConfig)
                .sslContext(sslContext)
                .hostnameVerifier((s, sslSession) -> true)
                .register(MultiPartFeature.class)
                .build();
    }
}
