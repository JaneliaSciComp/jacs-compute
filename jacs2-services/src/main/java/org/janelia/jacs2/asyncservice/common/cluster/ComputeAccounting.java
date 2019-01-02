package org.janelia.jacs2.asyncservice.common.cluster;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ProcessorHelper;
import org.janelia.jacs2.asyncservice.imagesearch.ColorDepthFileSearch;
import org.janelia.jacs2.cdi.qualifier.StrPropertyValue;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.security.Group;
import org.janelia.model.security.Subject;
import org.janelia.model.security.util.SubjectUtils;
import org.janelia.model.service.JacsServiceData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A centralized utility class for determining how to bill people for compute time.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Singleton
public class ComputeAccounting {

    private static final String DEFAULT_GROUP_NAME = "jacs";
    private static final String LSF_GROUP_COMMAND = "lsfgroup";

    private final Logger log;
    private final LegacyDomainDao dao;

    @Inject
    public ComputeAccounting(LegacyDomainDao dao,
                             @StrPropertyValue(name = "service.colorDepthSearch.filepath") String rootPath,
                             Logger logger) {
        this.log = logger;
        this.dao = dao;
    }

    /**
     * A cache which keeps track of group information so we don't need to make system calls 
     * and database lookups for every grid submission. 
     */
    private LoadingCache<String, String> computeAccounts = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(30, TimeUnit.MINUTES)
            .build(
                new CacheLoader<String, String>() {
                  public String load(String subjectKey) throws Exception {
                      if (subjectKey.startsWith("group")) {
                          Group group = dao.getGroupByNameOrKey(subjectKey);
                          String ldapGroup = group==null?null:group.getLdapGroupName();
                          log.debug("Got LDAP group "+ldapGroup+" for subject "+subjectKey);
                          return ldapGroup;
                      } else {
                          String username = SubjectUtils.getSubjectName(subjectKey);
                          String lsfGroup = runLsfGroup(username);
                          if (!StringUtils.isBlank(lsfGroup)) {
                              log.debug("Got LSF group "+lsfGroup+" for subject "+subjectKey);
                              return lsfGroup;
                          }
                          else {
                              log.debug("Got no group "+lsfGroup+" for subject "+subjectKey);
                              return null;
                          }
                      }
                  }
                });
    
    /**
     * Runs the lsfgroup command-line utility and returns the group that is reported. 
     * @param username
     * @return
     * @throws IOException
     */
    private String runLsfGroup(String username) throws IOException {
        
        List<String> cmd = new ArrayList<>();
        cmd.add(LSF_GROUP_COMMAND);
        cmd.add(username);
        
        ProcessBuilder processBuilder = new ProcessBuilder(cmd);
        processBuilder.redirectErrorStream(true);

        Process p = processBuilder.start();

        try (BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            String line;
            while ((line = input.readLine()) != null) {
                if (StringUtils.isBlank(line)) continue;
                if (line.contains("not valid")) return null;
                return line.trim();
            }
        }

        return null;
    }
    
    /**
     * Returns the compute group to use for billing the given subject.
     * @param subjectKey
     * @return
     */
    private synchronized String getComputeGroup(String subjectKey) {
        try {
            return computeAccounts.get(subjectKey);
        } catch (InvalidCacheLoadException | ExecutionException e) {
            log.error("Error getting compute group account for "+subjectKey, e);
            return null;
        }
    }

    /**
     * Returns the compute account to use for billing the given subject. For example "scicompsoft-rokicki",
     * or "dickson-dicksonlab". If there is no group affiliation available for the subject, then 
     * the default jacs group will be used.
     * @param subjectKey
     * @return
     */
    private synchronized String getDiscountedComputeAccount(String subjectKey) {
        String group = getComputeGroup(subjectKey);
        if (StringUtils.isBlank(group)) {
            log.warn("Defaulting to compute account '"+DEFAULT_GROUP_NAME+"' for subject '"+subjectKey+"'");
            group = DEFAULT_GROUP_NAME;
        }
        String subjectName = SubjectUtils.getSubjectName(subjectKey);
        return group+"-"+subjectName; // group-subject is only for discounted users
    }

    /**
     * Returns the compute account to use for billing during the given service invocation.
     * @param serviceContext
     * @return
     */
    public String getComputeAccount(JacsServiceData serviceContext) {
        String serviceBillingAccount = ProcessorHelper.getGridBillingAccount(serviceContext.getResources());
        String billingAccount;
        if (StringUtils.isNotBlank(serviceBillingAccount)) {
            String ownerComputeGroup = getComputeGroup(serviceContext.getOwnerKey());
            // User provided a billing account
            Subject billedSubject = dao.getSubjectByNameOrKey(serviceBillingAccount);
            String billedComputeGroup;
            if (billedSubject != null) {
                billedComputeGroup = getComputeGroup(billedSubject.getKey());
            } else {
                // no subject entry found for the given billing account
                billedComputeGroup = serviceBillingAccount;
            }
            if (billedComputeGroup.equals(ownerComputeGroup)) {
                // the provided billing account matches the user's compute group so let it go
                log.info("Using provided billing account {}", serviceBillingAccount);
                billingAccount = billedComputeGroup;
            } else {
                // no match - check if the user has admin privileges
                Subject authenticatedUser = dao.getSubjectByNameOrKey(serviceContext.getAuthKey());
                if (SubjectUtils.isAdmin(authenticatedUser)) {
                    log.info("Admin user {} can use the provided billing account {}", serviceContext.getAuthKey(), serviceBillingAccount);
                    billingAccount = serviceBillingAccount;
                } else {
                    log.warn("User {} attempted to use billing account {} on behalf of {} without admin privileges",
                            serviceContext.getAuthKey(), serviceBillingAccount, serviceContext.getOwnerKey());
                    throw new SecurityException("Admin access is required to override compute account");
                }
            }
            log.info("Using provided billing account {}", billingAccount);
        } else {
            // Calculate billing account from job owner - for now assume that is the discounted billing
            billingAccount = getDiscountedComputeAccount(serviceContext.getOwnerKey());
            log.info("Using billing account {} for user {}", billingAccount, serviceContext.getOwnerKey());
        }
        return billingAccount;
    }
}
