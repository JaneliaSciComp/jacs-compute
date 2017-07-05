package org.janelia.jacs2.asyncservice.common;

import org.apache.commons.lang3.StringUtils;
import org.ggf.drmaa.Session;
import org.janelia.jacs2.asyncservice.qualifier.LSFClusterJob;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.slf4j.Logger;

import javax.inject.Inject;
import java.util.Map;

@LSFClusterJob
public class ExternalLSFDrmaaJobRunner extends AbstractExternalDrmaaJobRunner {

    @Inject
    public ExternalLSFDrmaaJobRunner(Session drmaaSession, JacsServiceDataPersistence jacsServiceDataPersistence, Logger logger) {
        super(drmaaSession, jacsServiceDataPersistence, logger);
    }

    protected String createNativeSpec(Map<String, String> jobResources) {
        StringBuilder nativeSpecBuilder = new StringBuilder();
        // append accountID for billing
        String billingAccount = getGridBillingAccount(jobResources);
        if (StringUtils.isNotBlank(billingAccount)) {
            nativeSpecBuilder.append("-P ").append(billingAccount).append(' ');
        }
        int nProcessingSlots = ProcessorHelper.getProcessingSlots(jobResources);
        if (nProcessingSlots > 1) {
            // append processing environment
            nativeSpecBuilder
                    .append("-n ").append(nProcessingSlots).append(' ')
                    .append("-R")
                    .append('"')
                    .append("affinity")
                    .append('[')
                    .append("core(1)")
                    .append(']')
                    .append('"')
                    .append(' ')
            ;
        }
        if (StringUtils.isNotBlank(jobResources.get("gridQueue"))) {
            nativeSpecBuilder.append("-q ").append(jobResources.get("gridQueue")).append(' ');
        }
        String gridNodeArchitecture = getGridNodeArchitecture(jobResources); // sandy, haswell, broadwell, avx2
        if (StringUtils.isNotBlank(gridNodeArchitecture)) {
            nativeSpecBuilder.append("-R")
                    .append('"')
                    .append("select")
                    .append('[')
                    .append(gridNodeArchitecture)
                    .append(']')
                    .append('"')
                    .append(' ')
            ;
        }
        String gridResourceLimits = getGridJobResourceLimits(jobResources);
        if (StringUtils.isNotBlank(gridResourceLimits)) {
            nativeSpecBuilder.append("-R")
                    .append('"')
                    .append(gridResourceLimits)
                    .append('"')
                    .append(' ')
            ;
        }
        return nativeSpecBuilder.toString();
    }

}
