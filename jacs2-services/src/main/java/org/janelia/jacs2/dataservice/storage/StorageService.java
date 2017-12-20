package org.janelia.jacs2.dataservice.storage;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.utils.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

public class StorageService {

    private static final Logger LOG = LoggerFactory.getLogger(StorageService.class);

    public static class StorageInfo {
        private final String storageLocation;
        private final String entryRootLocation;
        private final String entryRelativePath;

        public StorageInfo(String storageLocation, String entryRootLocation, String entryRelativePath) {
            this.storageLocation = storageLocation;
            this.entryRootLocation = entryRootLocation;
            this.entryRelativePath = entryRelativePath;
        }

        public String getStorageLocation() {
            return storageLocation;
        }

        public String getEntryRootLocation() {
            return entryRootLocation;
        }

        public String getEntryRelativePath() {
            return entryRelativePath;
        }
    }
    private final String storageServiceApiKey;

    @Inject
    public StorageService(@PropertyValue(name = "StorageService.ApiKey") String storageServiceApiKey) {
        this.storageServiceApiKey = storageServiceApiKey;
    }

    public InputStream getContentStream(String storageLocation, String entryName, String subject) {
        Client httpclient = null;
        try {
            httpclient = HttpUtils.createHttpClient();
            WebTarget target = httpclient.target(storageLocation).path("entry-content").path(entryName);

            Invocation.Builder requestBuilder = target.request()
                    .header("Authorization", "APIKEY " + storageServiceApiKey)
                    .header("JacsSubject", StringUtils.defaultIfBlank(subject, ""))
                    ;
            Response response = requestBuilder.get();
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new IllegalStateException(storageLocation + " returned with " + response.getStatus());
            }
            return response.readEntity(InputStream.class);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpclient != null) {
                httpclient.close();
            }
        }
    }

    public String putFileStream(String storageLocation, String entryName, String subject, InputStream dataStream) {
        Client httpclient = null;
        try {
            httpclient = HttpUtils.createHttpClient();
            WebTarget target = httpclient.target(storageLocation).path("file").path(entryName);

            Invocation.Builder requestBuilder = target.request()
                    .header("Authorization", "APIKEY " + storageServiceApiKey)
                    .header("JacsSubject", StringUtils.defaultIfBlank(subject, ""));
            Response response = requestBuilder.post(Entity.entity(dataStream, MediaType.APPLICATION_OCTET_STREAM_TYPE));
            if (response.getStatus() == Response.Status.CREATED.getStatusCode()) {
                return response.getHeaderString("Location");
            } else if (response.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
                LOG.warn("Entry {} already exists at {}", entryName, storageLocation);
                return response.getHeaderString("Location");
            }
            throw new IllegalStateException(storageLocation + " returned with " + response.getStatus());
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpclient != null) {
                httpclient.close();
            }
        }
    }

    public List<StorageInfo> listStorageContent(String storageLocation, String subject) {
        Client httpclient = null;
        try {
            httpclient = HttpUtils.createHttpClient();
            WebTarget target = httpclient.target(storageLocation).path("list");

            Invocation.Builder requestBuilder = target.request(MediaType.APPLICATION_JSON)
                    .header("Authorization", "APIKEY " + storageServiceApiKey)
                    .header("JacsSubject", StringUtils.defaultIfBlank(subject, ""))
                    ;
            Response response = requestBuilder.get();
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new IllegalStateException(storageLocation + " returned with " + response.getStatus());
            }
            List<JsonNode> storageCotent = response.readEntity(new GenericType<List<JsonNode>>(){});
            return storageCotent.stream()
                    .map(content -> new StorageInfo(storageLocation, content.get("rootLocation").asText(), content.get("nodeRelativePath").asText()))
                    .collect(Collectors.toList());
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpclient != null) {
                httpclient.close();
            }
        }
    }
}
