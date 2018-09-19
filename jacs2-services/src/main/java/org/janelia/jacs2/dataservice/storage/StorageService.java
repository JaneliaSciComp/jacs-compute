package org.janelia.jacs2.dataservice.storage;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
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
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class StorageService {

    private static final Logger LOG = LoggerFactory.getLogger(StorageService.class);

    private final String storageServiceApiKey;

    @Inject
    public StorageService(@PropertyValue(name = "StorageService.ApiKey") String storageServiceApiKey) {
        this.storageServiceApiKey = storageServiceApiKey;
    }

    public Optional<DataStorageInfo> lookupStorage(String storageServiceURL, String storageId, String storageName, String subject, String authToken) {
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
                PageResult<DataStorageInfo> storageInfoResult = response.readEntity(new GenericType<PageResult<DataStorageInfo>>(){});
                if (storageInfoResult.getResultList().size() > 1) {
                    LOG.warn("Request {} returned more than one result {} please refine the query", target, storageInfoResult);
                    return storageInfoResult.getResultList().stream().findFirst();
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

    public DataStorageInfo createStorage(String storageServiceURL, String storageName, List<String> storageTags, String subject, String authToken) {
        Client httpclient = null;
        try {
            httpclient = HttpUtils.createHttpClient();
            WebTarget target = httpclient.target(storageServiceURL);
            if (!StringUtils.endsWith(storageServiceURL, "/storage")) {
                target = target.path("storage");
            }
            Invocation.Builder requestBuilder = createRequestWithCredentials(target.request(MediaType.APPLICATION_JSON), subject, authToken);
            DataStorageInfo storageData = new DataStorageInfo();
            storageData.setName(storageName);
            storageData.setOwnerKey(subject);
            storageData.setStorageFormat("DATA_DIRECTORY");
            storageData.setStorageTags(storageTags);
            Response response = requestBuilder.post(Entity.json(storageData));
            int responseStatus = response.getStatus();
            if (responseStatus >= Response.Status.BAD_REQUEST.getStatusCode()) {
                LOG.warn("Error while trying to create storage {} for {} using {} - returned status {}", storageName, subject, target, responseStatus);
                throw new IllegalStateException("Error while trying to create storage " + storageName + " for " + subject);
            }
            return response.readEntity(DataStorageInfo.class);
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

    public InputStream getStorageContent(String storageEntryURL, String entryName, String subject, String authToken) {
        Client httpclient = null;
        try {
            httpclient = HttpUtils.createHttpClient();
            WebTarget target = httpclient.target(storageEntryURL);
            Invocation.Builder requestBuilder = createRequestWithCredentials(target.request(), subject, authToken);
            Response response = requestBuilder.get();
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new IllegalStateException(storageEntryURL + "for " + entryName + " returned with " + response.getStatus());
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

    public StorageEntryInfo putStorageContent(String storageURL, String entryName, String subject, String authToken, InputStream dataStream) {
        Client httpclient = null;
        try {
            httpclient = HttpUtils.createHttpClient();
            WebTarget target = httpclient.target(storageURL).path("file").path(entryName);
            Invocation.Builder requestBuilder = createRequestWithCredentials(target.request(), subject, authToken);
            Response response = requestBuilder.put(Entity.entity(dataStream, MediaType.APPLICATION_OCTET_STREAM_TYPE));
            String entryLocationUrl;
            if (response.getStatus() == Response.Status.CREATED.getStatusCode()) {
                entryLocationUrl = response.getHeaderString("Location");
                JsonNode storageNode = response.readEntity(new GenericType<JsonNode>(){});
                return extractStorageNodeFromJson(storageURL, entryLocationUrl, null, storageNode);
            } else {
                LOG.warn("Put content using {} return status {}", target, response.getStatus());
                throw new IllegalStateException(target.getUri() + " returned with " + response.getStatus());
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

    public List<StorageEntryInfo> listStorageContent(String storageLocationURL,
                                                     String storagePath,
                                                     String subject,
                                                     String authToken) {
        Client httpclient = null;
        try {
            httpclient = HttpUtils.createHttpClient();
            WebTarget target = httpclient.target(storageLocationURL).path("list");
            if (StringUtils.isNotBlank(storagePath)) {
                target = target.path(storagePath);
            }
            Invocation.Builder requestBuilder = createRequestWithCredentials(
                    target.request(MediaType.APPLICATION_JSON), subject, authToken);
            Response response = requestBuilder.get();
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new IllegalStateException(target.getUri() + " returned with " + response.getStatus());
            }
            List<JsonNode> storageCotent = response.readEntity(new GenericType<List<JsonNode>>(){});
            return storageCotent.stream()
                    .map(content -> extractStorageNodeFromJson(storageLocationURL, null, storagePath, content))
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

    public void removeStorageContent(String storageLocationURL, String storagePath, String subject, String authToken) {
        Client httpclient = null;
        try {
            httpclient = HttpUtils.createHttpClient();
            WebTarget target = httpclient.target(storageLocationURL);
            if (StringUtils.isNotBlank(storagePath)) {
                target = target.path(storagePath);
            }
            Invocation.Builder requestBuilder = createRequestWithCredentials(
                    target.request(MediaType.APPLICATION_JSON), subject, authToken);
            Response response = requestBuilder.delete();
            if (response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
                throw new IllegalStateException(target.getUri() + " returned with " + response.getStatus());
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

    private Invocation.Builder createRequestWithCredentials(Invocation.Builder requestBuilder, String jacsPrincipal, String authToken) {
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

    private StorageEntryInfo extractStorageNodeFromJson(String storageUrl, String storageEntryUrl, String storagePath, JsonNode jsonNode) {
        JsonNode storageIdNode = jsonNode.get("storageId");
        JsonNode storageRootLocationNode = jsonNode.get("storageRootLocation");
        JsonNode storageRootPathURINode = jsonNode.get("storageRootPathURI");
        JsonNode nodeAccessURLNode = jsonNode.get("nodeAccessURL");
        JsonNode nodeRelativePathNode = jsonNode.get("nodeRelativePath");
        JsonNode collectionFlagNode = jsonNode.get("collectionFlag");
        String storageId = null;
        if (storageIdNode != null && !storageIdNode.isNull()) {
            storageId = storageIdNode.asText();
        }
        String actualEntryURL;
        if (nodeAccessURLNode != null && StringUtils.isNotBlank(nodeAccessURLNode.asText())) {
            actualEntryURL = nodeAccessURLNode.asText();
        } else if (StringUtils.isNotBlank(storageEntryUrl)) {
            actualEntryURL = storageEntryUrl;
        } else {
            if (StringUtils.isNotBlank(storagePath)) {
                actualEntryURL = StringUtils.appendIfMissing(storageUrl, "/") + storagePath;
            } else {
                actualEntryURL = storageUrl;
            }
        }
        return new StorageEntryInfo(
                storageId,
                storageUrl,
                actualEntryURL,
                storageRootLocationNode.asText(),
                new StoragePathURI(storageRootPathURINode.asText()),
                nodeRelativePathNode.asText(),
                collectionFlagNode.asBoolean());
    }
}
