package org.janelia.jacs2.dataservice.storage;

import org.janelia.jacs2.utils.HttpUtils;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

public class StorageService {

    public List<String> listStorageContent(String storageLocation) {
        Client httpclient = null;
        try {
            httpclient = HttpUtils.createHttpClient();
            WebTarget target = httpclient.target(storageLocation).path("list");

            Response response = target.request(MediaType.APPLICATION_JSON)
                    .get();
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new IllegalStateException(storageLocation + " returned with " + response.getStatus());
            }
            return response.readEntity(new GenericType<List<String>>(){});
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpclient != null) {
                httpclient.close();
            }
        }
    }
}
