package org.janelia.jacs2.dataservice.storage;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.utils.HttpUtils;
import org.janelia.model.jacs2.page.PageResult;
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
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class StorageService {

    private static final Logger LOG = LoggerFactory.getLogger(StorageService.class);

    public static class StorageInfo {
        @JsonProperty("id")
        private String storageId;
        private String name;
        private String ownerKey;
        private String path;
        private String storageHost;
        private List<String> storageTags;
        private String connectionURL;
        private String storageFormat;

        public String getStorageId() {
            return storageId;
        }

        public void setStorageId(String storageId) {
            this.storageId = storageId;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getOwnerKey() {
            return ownerKey;
        }

        public void setOwnerKey(String ownerKey) {
            this.ownerKey = ownerKey;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public String getStorageHost() {
            return storageHost;
        }

        public void setStorageHost(String storageHost) {
            this.storageHost = storageHost;
        }

        public List<String> getStorageTags() {
            return storageTags;
        }

        public void setStorageTags(List<String> storageTags) {
            this.storageTags = storageTags;
        }

        public String getConnectionURL() {
            return connectionURL;
        }

        public void setConnectionURL(String connectionURL) {
            this.connectionURL = connectionURL;
        }

        public String getStorageFormat() {
            return storageFormat;
        }

        public void setStorageFormat(String storageFormat) {
            this.storageFormat = storageFormat;
        }
    }

    public static class StorageEntryInfo {
        private final String storageURL;
        private final String entryRootPrefix;
        private final String entryRootLocation;
        private final String entryRelativePath;
        private final boolean collectionFlag;

        public StorageEntryInfo(String storageURL, String entryRootLocation, String entryRootPrefix, String entryRelativePath, boolean collectionFlag) {
            this.storageURL = storageURL;
            this.entryRootLocation = entryRootLocation;
            this.entryRootPrefix = entryRootPrefix;
            this.entryRelativePath = entryRelativePath;
            this.collectionFlag = collectionFlag;
        }

        public String getStorageURL() {
            return storageURL;
        }

        public String getEntryRootLocation() {
            return entryRootLocation;
        }

        public String getEntryRootPrefix() {
            return entryRootPrefix;
        }

        public String getEntryRelativePath() {
            return entryRelativePath;
        }

        public String getEntryPath() {
            return Paths.get(entryRootPrefix, entryRelativePath).toString();
        }

        public boolean isCollectionFlag() {
            return collectionFlag;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("storageURL", storageURL)
                    .append("entryRootPrefix", entryRootPrefix)
                    .append("entryRootLocation", entryRootLocation)
                    .append("entryRelativePath", entryRelativePath)
                    .append("collectionFlag", collectionFlag)
                    .toString();
        }
    }
    private final String storageServiceApiKey;

    @Inject
    public StorageService(@PropertyValue(name = "StorageService.ApiKey") String storageServiceApiKey) {
        this.storageServiceApiKey = storageServiceApiKey;
    }

    public Optional<StorageInfo> lookupStorage(String storageServiceURL, String storageId, String storageName, String subject, String authToken) {
        Client httpclient = null;
        try {
            httpclient = HttpUtils.createHttpClient();
            WebTarget target = httpclient.target(storageServiceURL);
            if (!StringUtils.endsWith(storageServiceURL, "/storage")) {
                target = target.path("storage");
            }
            if (StringUtils.isNotBlank(storageId)) {
                target = target.queryParam("id", storageId);
            }
            if (StringUtils.isNotBlank(storageName)) {
                target = target.queryParam("name", storageName);
            }
            if (StringUtils.isNotBlank(subject)) {
                target = target.queryParam("ownerKey", subject);
            }
            Invocation.Builder requestBuilder = createRequestWithCredentials(target.request(MediaType.APPLICATION_JSON), subject, authToken);
            Response response = requestBuilder.get();
            int responseStatus = response.getStatus();
            if (responseStatus >= Response.Status.BAD_REQUEST.getStatusCode()) {
                LOG.warn("Request {} returned status {}", target, responseStatus);
                return Optional.empty();
            } else {
                PageResult<StorageInfo> storageInfoResult = response.readEntity(new GenericType<PageResult<StorageInfo>>(){});
                if (storageInfoResult.getResultList().size() > 0) {
                    LOG.warn("Request {} returned more than one result {} please refine the query", target, storageInfoResult);
                    return Optional.empty();
                } else {
                    return storageInfoResult.getResultList().stream().findFirst();
                }
            }
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

    public StorageInfo createStorage(String storageServiceURL, String storageName, String subject, String authToken) {
        Client httpclient = null;
        try {
            httpclient = HttpUtils.createHttpClient();
            WebTarget target = httpclient.target(storageServiceURL);
            if (!StringUtils.endsWith(storageServiceURL, "/storage")) {
                target = target.path("storage");
            }
            Invocation.Builder requestBuilder = createRequestWithCredentials(target.request(MediaType.APPLICATION_JSON), subject, authToken);
            StorageInfo storageData = new StorageInfo();
            storageData.setName(storageName);
            storageData.setOwnerKey(subject);
            Response response = requestBuilder.post(Entity.json(storageData));
            int responseStatus = response.getStatus();
            if (responseStatus >= Response.Status.BAD_REQUEST.getStatusCode()) {
                LOG.warn("Error while trying to create storage {} for {} using {} - returned status {}", storageName, subject, target, responseStatus);
                throw new IllegalStateException("Error while trying to create storage " + storageName + " for " + subject);
            }
            return response.readEntity(StorageInfo.class);
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

    public InputStream getStorageContent(String storageLocation, String entryName, String subject, String authToken) {
        Client httpclient = null;
        try {
            httpclient = HttpUtils.createHttpClient();
            WebTarget target = httpclient.target(storageLocation).path("entry_content").path(entryName);
            Invocation.Builder requestBuilder = createRequestWithCredentials(target.request(), subject, authToken);
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

    public StorageEntryInfo putStorageContent(String storageLocation, String entryName, String subject, String authToken, InputStream dataStream) {
        Client httpclient = null;
        try {
            httpclient = HttpUtils.createHttpClient();
            WebTarget target = httpclient.target(storageLocation).path("file").path(entryName);
            Invocation.Builder requestBuilder = createRequestWithCredentials(target.request(), subject, authToken);
            Response response = requestBuilder.post(Entity.entity(dataStream, MediaType.APPLICATION_OCTET_STREAM_TYPE));
            String entryLocationUrl;
            if (response.getStatus() == Response.Status.CREATED.getStatusCode()) {
                entryLocationUrl = response.getHeaderString("Location");
            } else if (response.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
                LOG.warn("Entry {} already exists at {}", entryName, storageLocation);
                entryLocationUrl = response.getHeaderString("Location");
            } else {
                throw new IllegalStateException(storageLocation + " returned with " + response.getStatus());
            }
            JsonNode storageNode = response.readEntity(new GenericType<JsonNode>(){});
            return extractStorageNodeFromJson(entryLocationUrl, storageNode);
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

    public List<StorageEntryInfo> listStorageContent(String storageLocation, String subject, String authToken) {
        Client httpclient = null;
        try {
            httpclient = HttpUtils.createHttpClient();
            WebTarget target = httpclient.target(storageLocation).path("list");
            Invocation.Builder requestBuilder = createRequestWithCredentials(
                    target.request(MediaType.APPLICATION_JSON), subject, authToken);
            Response response = requestBuilder.get();
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new IllegalStateException(storageLocation + " returned with " + response.getStatus());
            }
            List<JsonNode> storageCotent = response.readEntity(new GenericType<List<JsonNode>>(){});
            return storageCotent.stream()
                    .map(content -> extractStorageNodeFromJson(storageLocation, content))
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

    Invocation.Builder createRequestWithCredentials(Invocation.Builder requestBuilder, String jacsPrincipal, String authToken) {
        Invocation.Builder requestWithCredentialsBuilder = requestBuilder;
        if (StringUtils.isNotBlank(authToken)) {
            requestWithCredentialsBuilder = requestWithCredentialsBuilder.header(
                    "Authorization",
                    "Bearer " + authToken);
        } else if (StringUtils.isNotBlank(storageServiceApiKey)) {
            requestWithCredentialsBuilder = requestWithCredentialsBuilder.header(
                    "Authorization",
                    "APIKEY " + storageServiceApiKey);
        }
        if (StringUtils.isNotBlank(jacsPrincipal)) {
            requestWithCredentialsBuilder = requestWithCredentialsBuilder.header(
                    "JacsSubject",
                    jacsPrincipal);
        }
        return requestWithCredentialsBuilder;
    }

    private StorageEntryInfo extractStorageNodeFromJson(String storageUrl, JsonNode jsonNode) {
        JsonNode rootLocation = jsonNode.get("rootLocation");
        JsonNode rootPrefix = jsonNode.get("rootPrefix");
        JsonNode nodeRelativePath = jsonNode.get("nodeRelativePath");
        JsonNode collectionFlag = jsonNode.get("collectionFlag");
        return new StorageEntryInfo(storageUrl, rootLocation.asText(), rootPrefix.asText(), nodeRelativePath.asText(), collectionFlag.asBoolean());
    }
}
