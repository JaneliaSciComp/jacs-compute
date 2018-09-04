package org.janelia.jacs2.asyncservice.sample.helpers;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.janelia.jacs2.asyncservice.exceptions.DiskQuotaException;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.domain.report.QuotaUsage;
import org.janelia.model.security.util.SubjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * This code is closely derived from similar code in TMOG in a class called JacsDataSetQuotaValidator, 
 * which was written by Eric Trautman. It was adapted for pipeline use by Konrad Rokicki.
 * 
 * This validator queries the JFS storage quota service to prevent processing
 * of samples for JACS data sets that have exceeded storage limits.
 *
 * Since this validation is nice-to-have (and not critical), any connection or data issues
 * with the quota service are simply logged.
 * Validation only fails (or warns) when a successfully parsed quota service response
 * indicates a quota has been exceeded (or is close to being exceeded).
 *
 * To reduce network traffic, quota service responses are cached for a default period of one hour.
 *
 * NOTE:
 * The quota service expects requests to contain a subject name instead of
 * a data set. This validator maps data sets to subject names based upon the
 * JACS convention of prefixing data set names with the owner's subject name
 * (e.g. the subject name for data set 'nerna_polarity_case_3' is 'nerna').
 *
 * @author Eric Trautman
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Singleton
public class QuotaValidator {

    private static final Logger log = LoggerFactory.getLogger(QuotaValidator.class);

    private static final HttpClient httpClient;
    static {
        MultiThreadedHttpConnectionManager multiThreadedHttpConnectionManager = new MultiThreadedHttpConnectionManager();
        HttpConnectionManagerParams params = new HttpConnectionManagerParams();
        params.setDefaultMaxConnectionsPerHost(20);
        multiThreadedHttpConnectionManager.setParams(params);
        httpClient = new HttpClient(multiThreadedHttpConnectionManager);
    }

    @PropertyValue(name = "Quota.ServiceURL")
    private String SERVICE_URL;

    @PropertyValue(name = "FileStore.CentralDir")
    private String FILESTORE_DIR;

    @PropertyValue(name = "Quota.FileStoreDir")
    private String QUOTA_DIR;

    @PropertyValue(name = "Quota.Validation")
    private boolean QUOTA_VALIDATION;

    private final static QuotaStatus UNDEFINED_QUOTA = new QuotaStatus();
    private static final Gson gson = new Gson();
    
    /**
     * The maximum amount of time (in milliseconds) between cache references before the cache should be cleared.
     * This is intended to keep the cache from getting stale.
     */
    private long clearCacheDuration = 60 * 1000; // one minute

    /** Time the cache was last referenced. */
    private long lastCacheAccessTime;

    /** Maps data set names to retrieved (cached) quota data. */
    private Map<String, QuotaStatus> subjectToQuotaMap;

    private LoadingCache<String, QuotaUsage> quotaReportCache;

    public QuotaValidator() {
        this.lastCacheAccessTime = System.currentTimeMillis();
        this.subjectToQuotaMap = new ConcurrentHashMap<>();
        
        this.quotaReportCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .build(
                    new CacheLoader<String, QuotaUsage>() {
                      public QuotaUsage load(String subjectName) throws Exception {
                        return QuotaValidator.this.fetchQuotaReport(subjectName);
                      }
                    });
    }

    /**
     * Checks the user's disk quota. If there is a problem, an exception is thrown.
     * @param subjectNameOrKey user whose disk quota needs to be checked
     * @throws DiskQuotaException user has exceeded their disk quota limit
     */
    public void validate(String subjectNameOrKey) throws DiskQuotaException {

        if (!QUOTA_VALIDATION) {
            return;
        }
        
        if (!FILESTORE_DIR.equals(QUOTA_DIR)) {
            return;
        }
        
        log.trace("Validating quota for "+subjectNameOrKey);
        
        clearCacheIfStale();
        
        String subjectName = SubjectUtils.getSubjectName(subjectNameOrKey);

        QuotaStatus dataSetQuota = subjectToQuotaMap.get(subjectName);
        if (dataSetQuota == null) {

            try {
                dataSetQuota = getQuotaStatus(subjectName);
            } 
            catch (Exception e) {
                dataSetQuota = UNDEFINED_QUOTA;
                log.warn("failed to validate quota for data set '" + subjectName + "', ignoring error", e);
            }

            subjectToQuotaMap.put(subjectName, dataSetQuota);
        }
        
        if (dataSetQuota.isFail()) {
            log.warn("User "+subjectName+" has failed quota check: "+dataSetQuota.getDetails());
            throw new DiskQuotaException("Cannot create additional files. "+dataSetQuota.getDetails());
        } 
        else if (dataSetQuota.isWarning()) {
            log.warn("User "+subjectName+" has received a quota warning: "+dataSetQuota.getDetails());
        }
        else {
            log.debug("Quota check succeeded.");
        }
    }
    
    private synchronized QuotaStatus getQuotaStatus(String subjectName) {

        QuotaStatus dataSetQuota;
        final String url = String.format("%s/%s/%s", SERVICE_URL, "status", subjectName);
        GetMethod method = new GetMethod(url);
        try {

            log.trace("getQuota: sending GET " + url + " for user '" + subjectName + "'");

            int responseCode = httpClient.executeMethod(method);
            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw new IllegalArgumentException(
                        "HTTP request failed with response code " + responseCode + ".  " + getErrorContext(url));
            }

            dataSetQuota = QuotaStatus.fromJson(method.getResponseBodyAsString());

        } 
        catch (IOException e) {
            throw new IllegalArgumentException("HTTP request failed.  " + getErrorContext(url), e);
        } 
        catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse HTTP response.  " + getErrorContext(url), e);
        } 
        finally {
            method.releaseConnection();
        }

        log.trace("getQuota: retrieved " + dataSetQuota);
        return dataSetQuota;
    }

    public synchronized QuotaUsage getQuotaReport(String subjectName) throws Exception {
        log.info("Requesting quota usage report for "+subjectName);
        return quotaReportCache.get(subjectName);
    }
    
    public synchronized QuotaUsage fetchQuotaReport(String subjectName) {
        log.info("Fetching quota usage report for "+subjectName);
        
        QuotaUsage dataSetQuota = null;
        final String url = String.format("%s/%s/%s", SERVICE_URL, "report", subjectName);
        GetMethod method = new GetMethod(url);
        try {

            log.info("getQuota: sending GET " + url + " for user '" + subjectName + "'");

            int responseCode = httpClient.executeMethod(method);
            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw new IllegalArgumentException(
                        "HTTP request failed with response code " + responseCode + ".  " + getErrorContext(url));
            }
            
            Type type = new TypeToken<Map<String, QuotaUsage>>(){}.getType();
            Map<String, QuotaUsage> dataSetQuotas = gson.fromJson(method.getResponseBodyAsString(), type);
            if (dataSetQuotas.containsKey(subjectName)) {
                dataSetQuota = dataSetQuotas.get(subjectName);
            }

        } 
        catch (IOException e) {
            throw new IllegalArgumentException("HTTP request failed.  " + getErrorContext(url), e);
        } 
        catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse HTTP response.  " + getErrorContext(url), e);
        } 
        finally {
            method.releaseConnection();
        }

        log.info("getQuota: retrieved " + dataSetQuota);
        return dataSetQuota;
    }
    
    private String getErrorContext(String url) {
        return "Please verify the configured quota service URL '" + url +
               "' is accurate and that the corresponding service is available.";
    }
    
    /**
     * Clears the cache if it has not been accessed recently.
     */
    private synchronized void clearCacheIfStale() {

        if ((System.currentTimeMillis() - lastCacheAccessTime) > clearCacheDuration) {
            log.info("clearCacheIfStale: clearing cache containing " + subjectToQuotaMap.size() + " items");
            subjectToQuotaMap.clear();
        }

        lastCacheAccessTime = System.currentTimeMillis();
    }

    private static class QuotaStatus {

        private static final Gson gson = new Gson();
        private String state;
        private String details;

        public QuotaStatus() {
            this.state = "UNKNOWN";
            this.details = "Not available";
        }

        public static QuotaStatus fromJson(String queryResponseString) {
            return gson.fromJson(queryResponseString, QuotaStatus.class);
        }

        public boolean isFail() {
            return "FAIL".equalsIgnoreCase(state);
        }

        public boolean isWarning() {
            return "WARN".equalsIgnoreCase(state);
        }

        public String getDetails() {
            return details;
        }

        @Override
        public String toString() {
            return gson.toJson(this);
        }
    }

}