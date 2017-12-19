package org.janelia.jacs2.dataservice.storage;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.utils.HttpUtils;

import javax.inject.Inject;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.stream.Collectors;

public class StorageService {

    private final String storageServiceApiKey;

    @Inject
    public StorageService(@PropertyValue(name = "StorageService.ApiKey") String storageServiceApiKey) {
        this.storageServiceApiKey = storageServiceApiKey;
    }

    public List<String> listStorageContent(String storageLocation, String subject) {
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
            return storageCotent.stream().map(content -> content.get("nodePath").asText()).collect(Collectors.toList());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpclient != null) {
                httpclient.close();
            }
        }
    }
}
