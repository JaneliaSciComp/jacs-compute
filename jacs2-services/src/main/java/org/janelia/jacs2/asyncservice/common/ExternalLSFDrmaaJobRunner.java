package org.janelia.jacs2.asyncservice.common;

import org.apache.commons.lang3.StringUtils;
import org.ggf.drmaa.Session;
import org.janelia.jacs2.asyncservice.qualifier.LSFDrmaaJob;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.slf4j.Logger;

import javax.inject.Inject;
import java.util.Map;

@LSFDrmaaJob
public class ExternalLSFDrmaaJobRunner extends AbstractExternalDrmaaJobRunner {

    @Inject
    public ExternalLSFDrmaaJobRunner(Session drmaaSession, JacsServiceDataPersistence jacsServiceDataPersistence, Logger logger) {
        super(drmaaSession, jacsServiceDataPersistence, logger);
    }

    protected String createNativeSpec(Map<String, String> jobResources) {
        StringBuilder nativeSpecBuilder = new StringBuilder();
        // append accountID for billing
        String billingAccount = ProcessorHelper.getGridBillingAccount(jobResources);
        if (StringUtils.isNotBlank(billingAccount)) {
            nativeSpecBuilder.append("-P ").append(billingAccount).append(' ');
        }
        int nProcessingSlots = ProcessorHelper.getProcessingSlots(jobResources);
        StringBuilder resourceBuffer = new StringBuilder();
        if (nProcessingSlots > 1) {
            // append processing environment
            nativeSpecBuilder
                    .append("-n ").append(nProcessingSlots).append(' ');
            resourceBuffer
                    .append("affinity")
                    .append('[')
                    .append("core(1)")
                    .append(']');
            ;
        }
        long softJobDurationInMins = ProcessorHelper.getSoftJobDurationLimitInSeconds(jobResources) / 60;
        if (softJobDurationInMins > 0) {
            nativeSpecBuilder.append("-We 0:").append(softJobDurationInMins).append(' ');
        }
        long hardJobDurationInMins = ProcessorHelper.getHardJobDurationLimitInSeconds(jobResources) / 60;
        if (hardJobDurationInMins > 0) {
            nativeSpecBuilder.append("-W 0:").append(hardJobDurationInMins).append(' ');
        }
        if (StringUtils.isNotBlank(jobResources.get("gridQueue"))) {
            nativeSpecBuilder.append("-q ").append(jobResources.get("gridQueue")).append(' ');
        }
        StringBuilder selectResourceBuffer = new StringBuilder();
        String gridNodeArchitecture = ProcessorHelper.getCPUType(jobResources); // sandy, haswell, broadwell, avx2
        if (StringUtils.isNotBlank(gridNodeArchitecture)) {
            selectResourceBuffer.append(gridNodeArchitecture);
        }
        String gridResourceLimits = ProcessorHelper.getGridJobResourceLimits(jobResources);
        if (StringUtils.isNotBlank(gridResourceLimits)) {
            if (selectResourceBuffer.length() > 0) {
                selectResourceBuffer.append(',');
            }
            selectResourceBuffer.append(gridResourceLimits);
        }
        if (selectResourceBuffer.length() > 0) {
            if (resourceBuffer.length() > 0) {
                resourceBuffer.append(' ');
            }
            resourceBuffer
                    .append("select")
                    .append('[')
                    .append(selectResourceBuffer)
                    .append(']');
            ;
        }
        if (resourceBuffer.length() > 0) {
            nativeSpecBuilder.append("-R ")
                    .append('"')
                    .append(resourceBuffer)
                    .append('"')
                    .append(' ')
                    ;
        }
        return nativeSpecBuilder.toString();
    }

}
