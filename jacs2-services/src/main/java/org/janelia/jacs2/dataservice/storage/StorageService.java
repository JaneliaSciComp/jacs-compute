package org.janelia.jacs2.dataservice.storage;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.glassfish.jersey.client.ClientProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.utils.HttpUtils;
import org.janelia.model.domain.report.QuotaUsage;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.rendering.Streamable;
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

    private final String masterStorageServiceURL;
    private final String storageServiceApiKey;

    @Inject
    public StorageService(@PropertyValue(name = "StorageService.URL") String masterStorageServiceURL,
                          @PropertyValue(name = "StorageService.ApiKey") String storageServiceApiKey) {
        this.masterStorageServiceURL = masterStorageServiceURL;
        this.storageServiceApiKey = storageServiceApiKey;
    }

    public Optional<QuotaUsage> fetchQuotaForUser(String volumeName, String userKey) {
        Client httpclient = HttpUtils.createHttpClient();
        try {
            WebTarget target = httpclient.target(masterStorageServiceURL)
                    .path("storage/quota")
                    .path(volumeName)
                    .path("report")
                    .queryParam("subjectName", userKey);
            Invocation.Builder requestBuilder = createRequestWithCredentials(target.request(MediaType.APPLICATION_JSON), userKey, null);
            Response response = requestBuilder.get();
            int responseStatus = response.getStatus();
            if (responseStatus >= Response.Status.BAD_REQUEST.getStatusCode()) {
                LOG.warn("Request {} returned status {} while trying to retrieved the quota for {} on {}", target, responseStatus, userKey, volumeName);
                throw new IllegalStateException("Request " + target.getUri() + " returned an invalid response while trying to get the quota for " + userKey + " on " + volumeName);
            } else {
                List<QuotaUsage> quotaReport = response.readEntity(new GenericType<List<QuotaUsage>>(){});
                return quotaReport.stream().findFirst();
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            httpclient.close();
        }
    }

    public Optional<DataStorageInfo> lookupDataStorage(String storageURI, String storageId, String storageName, String storagePath, String subjectKey, String authToken) {
        Client httpclient = HttpUtils.createHttpClient();
        try {
            WebTarget target;
            if (StringUtils.isBlank(storageURI)) {
                target = httpclient.target(masterStorageServiceURL);
            } else {
                target = httpclient.target(storageURI);
            }
            if (!StringUtils.endsWith(storageURI, "/storage")) {
                target = target.path("storage");
            }
            StringBuilder storageIdentifiersBuilder = new StringBuilder();
            if (StringUtils.isNotBlank(storageId)) {
                target = target.queryParam("id", storageId);
                storageIdentifiersBuilder.append("storageId=").append(storageId).append(' ');
            }
            if (StringUtils.isNotBlank(storageName)) {
                target = target.queryParam("name", storageName);
                if (storageIdentifiersBuilder.length() > 0) {
                    storageIdentifiersBuilder.append(',');
                }
                storageIdentifiersBuilder.append("storageName=").append(storageName).append(' ');
            }
            if (StringUtils.isNotBlank(storagePath)) {
                target = target.queryParam("storagePath", storagePath);
                if (storageIdentifiersBuilder.length() > 0) {
                    storageIdentifiersBuilder.append(',');
                }
                storageIdentifiersBuilder.append("storagePath=").append(storagePath).append(' ');
            }
            if (StringUtils.isNotBlank(subjectKey)) {
                target = target.queryParam("ownerKey", subjectKey);
            }
            Invocation.Builder requestBuilder = createRequestWithCredentials(target.request(MediaType.APPLICATION_JSON), subjectKey, authToken);
            Response response = requestBuilder.get();
            int responseStatus = response.getStatus();
            if (responseStatus >= Response.Status.BAD_REQUEST.getStatusCode()) {
                LOG.error("Lookup data storage request {} returned status {} while trying to get the storage for storageId = {}, storageName={}, storagePath={}", target, responseStatus, storageId, storageName, storagePath);
                StringBuilder messageBuilder = new StringBuilder("Cannot locate any storage ");
                if (storageIdentifiersBuilder.length() > 0) {
                    messageBuilder.append("for ")
                            .append(storageIdentifiersBuilder);
                }
                messageBuilder.append("at ").append(target.getUri())
                        .append(". The attempt to connect to the storage server returned with an invalid status code")
                        .append('(').append(responseStatus).append(')');
                ;
                throw new IllegalStateException(messageBuilder.toString());
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
            httpclient.close();
        }
    }

    public Optional<JadeStorageVolume> lookupStorageVolumes(String storageId, String storageName, String storagePath, String subjectKey, String authToken) {
        Client httpclient = HttpUtils.createHttpClient();
        try {
            WebTarget target = httpclient.target(masterStorageServiceURL)
                    .path("storage_volumes");
            if (StringUtils.isNotBlank(storageId)) {
                target = target.queryParam("id", storageId);
            }
            if (StringUtils.isNotBlank(storageName)) {
                target = target.queryParam("name", storageName);
            }
            if (StringUtils.isNotBlank(storagePath)) {
                target = target.queryParam("dataStoragePath", storagePath);
            }
            if (StringUtils.isNotBlank(subjectKey)) {
                target = target.queryParam("ownerKey", subjectKey);
            }
            Invocation.Builder requestBuilder = createRequestWithCredentials(target.request(MediaType.APPLICATION_JSON), subjectKey, authToken);
            Response response = requestBuilder.get();
            int responseStatus = response.getStatus();
            if (responseStatus >= Response.Status.BAD_REQUEST.getStatusCode()) {
                LOG.error("Lookup storage volume request {} returned status {} while trying to get the storage for storageId = {}, storageName={}, storagePath={}", target, responseStatus, storageId, storageName, storagePath);
                return Optional.empty();
            } else {
                PageResult<JadeStorageVolume> storageInfoResult = response.readEntity(new GenericType<PageResult<JadeStorageVolume>>(){});
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
            httpclient.close();
        }
    }

    public DataStorageInfo createStorage(String storageServiceURL, String storageName, List<String> storageTags, String subject, String authToken) {
        Client httpclient = HttpUtils.createHttpClient();
        try {
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
            httpclient.close();
        }
    }

    public InputStream getStorageContent(String storageURI, String subject, String authToken) {
        Client httpclient = HttpUtils.createHttpClient();
        try {
            WebTarget target = httpclient.target(storageURI);
            Invocation.Builder requestBuilder = createRequestWithCredentials(target.request(), subject, authToken);
            Response response = requestBuilder.get();
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new IllegalStateException(storageURI + " returned with " + response.getStatus());
            }
            return response.readEntity(InputStream.class);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            httpclient.close();
        }
    }

    public InputStream getStorageFolderContent(String storageURI, long offset, long size, boolean sortedContent, String filter, String subject, String authToken) {
        Client httpclient = HttpUtils.createHttpClient();
        try {
            WebTarget target = httpclient.target(storageURI)
                    .queryParam("alwaysArchive", true)
                    .queryParam("useNaturalSort", sortedContent)
                    .queryParam("startEntryIndex", offset)
                    .queryParam("entryPattern", filter)
                    .queryParam("entriesCount", size);
            Invocation.Builder requestBuilder = createRequestWithCredentials(target.request(), subject, authToken);
            Response response = requestBuilder.get();
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new IllegalStateException(storageURI + " returned with " + response.getStatus());
            }
            return response.readEntity(InputStream.class);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            httpclient.close();
        }
    }

    public StorageEntryInfo putStorageContent(String storageURI, String entryName, String subject, String authToken, InputStream dataStream) {
        Client httpclient = HttpUtils.createHttpClient();
        try {
            WebTarget target = httpclient
                    .target(storageURI).path("data_content")
                    .property(ClientProperties.CHUNKED_ENCODING_SIZE, 1024*1024)
                    .property(ClientProperties.REQUEST_ENTITY_PROCESSING, "CHUNKED")
                    .path(entryName);
            Invocation.Builder requestBuilder = createRequestWithCredentials(target.request(), subject, authToken);
            Response response = requestBuilder.put(Entity.entity(dataStream, MediaType.APPLICATION_OCTET_STREAM_TYPE));
            String entryLocationUrl;
            if (response.getStatus() == Response.Status.CREATED.getStatusCode()) {
                entryLocationUrl = response.getHeaderString("Location");
                JsonNode storageNode = response.readEntity(new GenericType<JsonNode>(){});
                return extractStorageNodeFromJson(storageURI, entryLocationUrl, null, storageNode);
            } else {
                LOG.warn("Put content using {} return status {}", target, response.getStatus());
                throw new IllegalStateException(target.getUri() + " returned with " + response.getStatus());
            }
        } catch (IllegalStateException e) {
            LOG.error("Exception thrown while uploading content to {}, {}", storageURI, entryName, e);
            throw e;
        } catch (Exception e) {
            LOG.error("Exception thrown while uploading content to {}, {}", storageURI, entryName, e);
            throw new IllegalStateException(e);
        } finally {
            httpclient.close();
        }
    }

    public List<StorageEntryInfo> listStorageContent(String storageURI,
                                                     String storagePath,
                                                     String subject,
                                                     String authToken,
                                                     int level,
                                                     long offset,
                                                     int length) {
        Client httpclient = HttpUtils.createHttpClient();
        try {
            WebTarget target = httpclient.target(storageURI).path("list");
            if (StringUtils.isNotBlank(storagePath)) {
                target = target.path(storagePath);
            }
            if (level > 0) {
                target = target.queryParam("depth", level);
            }
            if (offset > 0) {
                target = target.queryParam("offset", offset);
            }
            if (length > 0) {
                target = target.queryParam("length", length);
            }
            Invocation.Builder requestBuilder = createRequestWithCredentials(
                    target.request(MediaType.APPLICATION_JSON), subject, authToken);
            Response response = requestBuilder.get();
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new IllegalStateException(target.getUri() + " returned with " + response.getStatus());
            }
            List<JsonNode> storageCotent = response.readEntity(new GenericType<List<JsonNode>>(){});
            return storageCotent.stream()
                    .map(content -> extractStorageNodeFromJson(storageURI, null, storagePath, content))
                    .collect(Collectors.toList());
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            httpclient.close();
        }
    }

    public void removeStorageContent(String storageURI, String storagePath, String subject, String authToken) {
        Client httpclient = HttpUtils.createHttpClient();
        try {
            WebTarget target = httpclient.target(storageURI);
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
            httpclient.close();
        }
    }

    public String getEntryURI(String storageURI, String entryName) {
        return StringUtils.appendIfMissing(storageURI, "/") + "data_content/" + entryName;
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
        JsonNode sizeNode = jsonNode.get("size");
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
                sizeNode.asLong(),
                collectionFlagNode.asBoolean());
    }
}
