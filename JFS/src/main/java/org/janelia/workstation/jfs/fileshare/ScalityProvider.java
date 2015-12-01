package org.janelia.workstation.jfs.fileshare;


import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.http.Header;
import org.janelia.workstation.jfs.utils.MeasuringInputStreamEntity;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by schauderd on 6/26/15.
 */
public class ScalityProvider extends Provider {
    private Map<String,String> rings;
    private HttpClient httpClient;

    public ScalityProvider() {
        super();
    }

    @Override
    public void init() {
        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setMaxTotal(100);
        connManager.setDefaultMaxPerRoute(1);
        httpClient = HttpClients.custom()
                .setConnectionManager(connManager)
                .build();
    }

    protected String getUrlFromBPID(String bpid, String ringKey, boolean local) {
        if (rings.get(ringKey) != null) {
            if (local) {
                if (ringKey.equals("replicated")) {
                    return "http://localhost:81/proxy/bparc2/" + bpid;
                } else {
                    return "http://localhost:81/proxy/bparc/" + bpid;
                }
            }
            return rings.get(ringKey) + "/" + bpid;
        }
        return null;
    }

    public Map<String,String> head(String path, String ring, boolean local) throws WebApplicationException {

        try {
            final String url = getUrlFromBPID(path, ring, local);

            HttpHead head = new HttpHead(url);
            HttpResponse res = httpClient.execute(head);
            EntityUtils.consumeQuietly(res.getEntity());

            int statusCode = res.getStatusLine().getStatusCode();
            switch (Response.Status.fromStatusCode(statusCode)) {
                case OK:
                    break;
                case NOT_FOUND:
                    throw new WebApplicationException(Response.Status.NOT_FOUND);
                default:
                    throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
            }

            Map<String,String> headerMap = new HashMap<>();
            for(Header header : res.getAllHeaders()) {
                headerMap.put(header.getName(), header.getValue());
            }

            return headerMap;
        }
        catch (WebApplicationException e) {
            throw e;
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    public long put(InputStream input, String path, String ring, boolean local) throws WebApplicationException {
        boolean compress = false; // for now ignore
        try {
            final String url = getUrlFromBPID(path, ring, local);

            HttpPut put = new HttpPut(url);
            put.addHeader(HttpHeaders.CONTENT_TYPE, "application/octet-stream");

            MeasuringInputStreamEntity mise = new MeasuringInputStreamEntity(input, compress);
            put.setEntity(mise);
            HttpResponse res = httpClient.execute(put);
            EntityUtils.consumeQuietly(res.getEntity());

            int statusCode = res.getStatusLine().getStatusCode();

            switch (Response.Status.fromStatusCode(statusCode)) {
                case OK:
                    break;
                default:
                    throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
            }

            return mise.getContentLength();
        }
        catch (WebApplicationException e) {
            throw e;
        }
        catch (Exception e) {
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    public InputStream get(String path, String ring, boolean local) throws WebApplicationException {
        boolean decompress = false; // for now ignore
        try {
            final String url = getUrlFromBPID(path, ring, local);
            System.out.println (url);
            HttpGet get = new HttpGet(url);
            final HttpResponse res = httpClient.execute(get);
            int statusCode = res.getStatusLine().getStatusCode();

            switch (Response.Status.fromStatusCode(statusCode)) {
                case OK:
                    break;
                case NOT_FOUND:
                    throw new WebApplicationException(Response.Status.NOT_FOUND);
                default:
                    throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
            }

            InputStream input = res.getEntity().getContent();
            if (decompress) {
                input = new BZip2CompressorInputStream(input);
            }

            return input;
        }
        catch (WebApplicationException e) {
            throw e;
        }
        catch (Exception e) {
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    public boolean delete(String path, String ring, boolean local) throws WebApplicationException {

        try {
            final String url = getUrlFromBPID(path, ring, local);

            HttpDelete get = new HttpDelete(url);
            final HttpResponse res = httpClient.execute(get);

            // Have to consume the entity, otherwise the connection is not returned back to the pool
            EntityUtils.consumeQuietly(res.getEntity());

            int statusCode = res.getStatusLine().getStatusCode();

            switch (Response.Status.fromStatusCode(statusCode)) {
                case OK:
                    return true;
                case NOT_FOUND:
                    // Scality always returns OK, even if the object doesn't exist, but maybe in the future that could change.
                    throw new WebApplicationException(Response.Status.NOT_FOUND);
                default:
                    return false;
            }
        }
        catch (WebApplicationException e) {
            throw e;
        }
        catch (Exception e) {
            // Fail silently because we can always retry the deletion.
            return false;
        }
    }

    public Map<String, String> getRings() {
        return rings;
    }

    public void setRings(Map<String, String> rings) {
        this.rings = rings;
    }
}
