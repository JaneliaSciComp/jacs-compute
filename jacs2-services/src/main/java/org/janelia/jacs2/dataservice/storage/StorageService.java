package org.janelia.jacs2.dataservice.storage;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.client.ClientProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.utils.HttpUtils;
import org.janelia.jacsstorage.clients.api.JadeStorageAttributes;
import org.janelia.jacsstorage.clients.api.JadeStorageVolume;
import org.janelia.jacsstorage.clients.api.StorageEntryInfo;
import org.janelia.jacsstorage.clients.api.StoragePathURI;
import org.janelia.model.domain.report.QuotaUsage;
import org.janelia.model.jacs2.page.PageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Dependent
public class StorageService {

    private static final Logger LOG = LoggerFactory.getLogger(StorageService.class);

    private final String masterStorageServiceURL;
    private final String storageServiceApiKey;

    @Inject
    StorageService(@PropertyValue(name = "StorageService.URL") String masterStorageServiceURL,
                   @PropertyValue(name = "StorageService.ApiKey") String storageServiceApiKey) {
        this.masterStorageServiceURL = masterStorageServiceURL;
        this.storageServiceApiKey = storageServiceApiKey;
    }

    public Optional<QuotaUsage> fetchQuotaForUser(String volumeName, String userKey, JadeStorageAttributes storageOptions) {
        Client httpclient = HttpUtils.createHttpClient();
        try {
            WebTarget target = httpclient.target(masterStorageServiceURL)
                    .path("storage/quota")
                    .path(volumeName)
                    .path("report")
                    .queryParam("subjectName", userKey);
            Invocation.Builder requestBuilder = createRequestWithCredentials(target.request(MediaType.APPLICATION_JSON), userKey, null, storageOptions);
            Response response = requestBuilder.get();
            int responseStatus = response.getStatus();
            if (responseStatus >= Response.Status.BAD_REQUEST.getStatusCode()) {
                LOG.warn("Request {} returned status {} while trying to retrieved the quota for {} on {}", target, responseStatus, userKey, volumeName);
                throw new IllegalStateException("Request " + target.getUri() + " returned an invalid response while trying to get the quota for " + userKey + " on " + volumeName);
            } else {
                List<QuotaUsage> quotaReport = response.readEntity(new GenericType<List<QuotaUsage>>() {
                });
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

    List<DataStorageInfo> lookupDataBundle(String storageURI,
                                           String storageId,
                                           String storageName,
                                           String storagePath,
                                           String subjectKey,
                                           String authToken,
                                           JadeStorageAttributes storageOptions) {
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
            LOG.debug("Lookup data bundles with {}", target.getUri());
            Invocation.Builder requestBuilder = createRequestWithCredentials(target.request(MediaType.APPLICATION_JSON), subjectKey, authToken, storageOptions);
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
                PageResult<DataStorageInfo> storageInfoResult = response.readEntity(new GenericType<PageResult<DataStorageInfo>>() {
                });
                return storageInfoResult.getResultList();
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            httpclient.close();
        }
    }

    public List<JadeStorageVolume> findStorageVolumes(String storagePath, String subjectKey, String authToken, JadeStorageAttributes storageAttributes) {
        return lookupStorageVolumes(null, null, storagePath, subjectKey, authToken, storageAttributes).stream()
                .filter(storageVolume -> {
                    if ("S3".equals(storageVolume.getStorageType()) &&
                            (StringUtils.startsWith(storagePath, "https://") ||
                                    StringUtils.startsWith(storagePath, "s3://"))) {
                        return true;
                    } else {
                        // the storagePath must match volume's physical root location or volume's binding
                        return storagePath.equals(storageVolume.getStorageVirtualPath())
                                || storagePath.equals(storageVolume.getBaseStorageRootDir())
                                || storagePath.startsWith(StringUtils.appendIfMissing(storageVolume.getStorageVirtualPath(), "/"))
                                || storagePath.startsWith(StringUtils.appendIfMissing(storageVolume.getBaseStorageRootDir(), "/"));
                    }
                })
                .collect(Collectors.toList());
    }

    private List<JadeStorageVolume> lookupStorageVolumes(String storageId, String storageName, String storagePath, String subjectKey, String authToken, JadeStorageAttributes storageOptions) {
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
            LOG.debug("Lookup storage volumes with {}", target.getUri());
            Invocation.Builder requestBuilder = createRequestWithCredentials(target.request(MediaType.APPLICATION_JSON), subjectKey, authToken, storageOptions);
            Response response = requestBuilder.get();
            int responseStatus = response.getStatus();
            if (responseStatus >= Response.Status.BAD_REQUEST.getStatusCode()) {
                LOG.error("Lookup storage volume request {} returned status {} while trying to get the storage for storageId = {}, storageName={}, storagePath={}", target, responseStatus, storageId, storageName, storagePath);
                return Collections.emptyList();
            } else {
                PageResult<JadeStorageVolume> storageInfoResult = response.readEntity(new GenericType<PageResult<JadeStorageVolume>>() {
                });
                return storageInfoResult.getResultList();
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            httpclient.close();
        }
    }

    public DataStorageInfo createStorage(String storageServiceURL, String storageName, List<String> storageTags, String subject, String authToken, JadeStorageAttributes storageOptions) {
        Client httpclient = HttpUtils.createHttpClient();
        try {
            WebTarget target = httpclient.target(storageServiceURL);
            if (!StringUtils.endsWith(storageServiceURL, "/storage")) {
                target = target.path("storage");
            }
            Invocation.Builder requestBuilder = createRequestWithCredentials(target.request(MediaType.APPLICATION_JSON), subject, authToken, storageOptions);
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

    public InputStream getStorageContent(String storageURI, String subject, String authToken, JadeStorageAttributes storageOptions) {
        Client httpclient = HttpUtils.createHttpClient();
        try {
            WebTarget target = httpclient.target(storageURI);
            Invocation.Builder requestBuilder = createRequestWithCredentials(target.request(), subject, authToken, storageOptions);
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

    /**
     * Tar the given files paginated with the offset/size, and then stream the tar file.
     *
     * @param storageURI    must point to data_content URL of a folder containing files
     * @param offset        index of first file to stream
     * @param size          number of files to stream
     * @param depth         directory tree depth
     * @param sortedContent sort files before streaming
     * @param filter        regex filename filter
     * @param subject       subject key (or null)
     * @param authToken     authentication token (or null)
     * @return stream of tar
     */
    public InputStream getStorageFolderContent(String storageURI,
                                               long offset,
                                               long size,
                                               long depth,
                                               boolean sortedContent,
                                               String filter,
                                               String subject,
                                               String authToken,
                                               JadeStorageAttributes storageOptions) {
        Client httpclient = HttpUtils.createHttpClient();
        try {
            WebTarget target = httpclient.target(storageURI)
                    .queryParam("alwaysArchive", true)
                    .queryParam("useNaturalSort", sortedContent)
                    .queryParam("startEntryIndex", offset)
                    .queryParam("maxDepth", depth)
                    .queryParam("noSize", true)
                    .queryParam("entryPattern", filter)
                    .queryParam("entriesCount", size);
            LOG.debug("Get content: {}", target);
            Invocation.Builder requestBuilder = createRequestWithCredentials(target.request(), subject, authToken, storageOptions);
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

    public StorageEntryInfo putStorageContent(String storageURI, String entryName, String subject, String authToken,
                                              JadeStorageAttributes storageOptions,
                                              InputStream dataStream) {
        Client httpclient = HttpUtils.createHttpClient();
        try {
            WebTarget target = httpclient
                    .target(storageURI).path("data_content")
                    .property(ClientProperties.CHUNKED_ENCODING_SIZE, 1024 * 1024)
                    .property(ClientProperties.REQUEST_ENTITY_PROCESSING, "CHUNKED")
                    .path(entryName);
            Invocation.Builder requestBuilder = createRequestWithCredentials(target.request(), subject, authToken, storageOptions);
            Response response = requestBuilder.put(Entity.entity(dataStream, MediaType.APPLICATION_OCTET_STREAM_TYPE));
            String entryLocationUrl;
            if (response.getStatus() == Response.Status.CREATED.getStatusCode()) {
                entryLocationUrl = response.getHeaderString("Location");
                JsonNode storageNode = response.readEntity(new GenericType<JsonNode>() {
                });
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
                                                     int depth,
                                                     long offset,
                                                     int length,
                                                     JadeStorageAttributes storageAttributes) {
        Client httpclient = HttpUtils.createHttpClient();
        try {
            WebTarget target = httpclient.target(storageURI).path("list");
            if (StringUtils.isNotBlank(storagePath)) {
                target = target.path(storagePath);
            }
            if (depth > 0) {
                target = target.queryParam("depth", depth);
            }
            if (offset > 0) {
                target = target.queryParam("offset", offset);
            }
            if (length > 0) {
                target = target.queryParam("length", length);
            }
            Invocation.Builder requestBuilder = createRequestWithCredentials(
                    target.request(MediaType.APPLICATION_JSON), subject, authToken, storageAttributes
            );
            Response response = requestBuilder.get();
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new IllegalStateException(target.getUri() + " returned with " + response.getStatus());
            }
            List<JsonNode> storageCotent = response.readEntity(new GenericType<List<JsonNode>>() {
            });
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

    public void removeStorageContent(String storageURI, String storagePath, String subject, String authToken, JadeStorageAttributes storageOptions) {
        Client httpclient = HttpUtils.createHttpClient();
        try {
            WebTarget target = httpclient.target(storageURI);
            if (StringUtils.isNotBlank(storagePath)) {
                target = target.path(storagePath);
            }
            Invocation.Builder requestBuilder = createRequestWithCredentials(
                    target.request(MediaType.APPLICATION_JSON), subject, authToken, storageOptions
            );
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

    public boolean exists(String storageURI, String subjectKey, String authToken, JadeStorageAttributes storageOptions) {
        Client httpclient = HttpUtils.createHttpClient();
        try {
            WebTarget target = httpclient.target(storageURI);
            Invocation.Builder requestBuilder = createRequestWithCredentials(
                    target.request(MediaType.APPLICATION_JSON), subjectKey, authToken, storageOptions
            );
            Response response = requestBuilder.head();
            if (response.getStatus() >= 200 && response.getStatus() < 300) {
                return true;
            } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                return false;
            } else {
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

    protected Invocation.Builder createRequestWithCredentials(Invocation.Builder requestBuilder, String jacsPrincipal, String authToken, JadeStorageAttributes storageOptions) {
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
        for (String storageAttribute : storageOptions.getAttributeNames()) {
            requestBuilder.header(storageAttribute, storageOptions.getAttributeValue(storageAttribute));
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
        JsonNode mimeTypeNode = jsonNode.get("mimeType");
        String mimeType = mimeTypeNode != null ? mimeTypeNode.asText() : null;
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
                collectionFlagNode.asBoolean(),
                mimeType);
    }
}
