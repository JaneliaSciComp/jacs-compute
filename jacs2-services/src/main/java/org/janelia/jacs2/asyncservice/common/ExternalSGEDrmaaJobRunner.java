package org.janelia.jacs2.asyncservice.common;

import org.apache.commons.lang3.StringUtils;
import org.ggf.drmaa.Session;
import org.janelia.jacs2.asyncservice.common.cluster.ComputeAccounting;
import org.janelia.jacs2.cdi.qualifier.BoolPropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.asyncservice.qualifier.SGEDrmaaJob;
import org.slf4j.Logger;

import javax.inject.Inject;
import java.util.Map;

@SGEDrmaaJob
public class ExternalSGEDrmaaJobRunner extends AbstractExternalDrmaaJobRunner {

    @Inject
    public ExternalSGEDrmaaJobRunner(Session drmaaSession, JacsServiceDataPersistence jacsServiceDataPersistence,
                                     ComputeAccounting accounting,
                                     @BoolPropertyValue(name = "service.cluster.requiresAccountInfo", defaultValue = true) boolean requiresAccountInfo,
                                     Logger logger) {
        super(drmaaSession, jacsServiceDataPersistence, accounting, requiresAccountInfo, logger);
    }

    protected String createNativeSpec(Map<String, String> jobResources, String billingAccount, String jobRunningDir) {
        StringBuilder nativeSpecBuilder = new StringBuilder();
        if (isRequiresAccountInfo()) {
            nativeSpecBuilder.append("-A ").append(billingAccount).append(' ');
        }
        int nProcessingSlots = ProcessorHelper.getProcessingSlots(jobResources);
        if (nProcessingSlots > 1) {
            // append processing environment
            nativeSpecBuilder.append("-pe batch ").append(nProcessingSlots).append(' ');
        }
        // append grid queue
        if (StringUtils.isNotBlank(jobResources.get("gridQueue"))) {
            nativeSpecBuilder.append("-q ").append(jobResources.get("gridQueue")).append(' ');
        }
        // append grid resource limits - the resource limits must be specified as a comma delimited list of <name>'='<value>, e.g.
        // gridResourceLimits: "short=true,scalityr=1,scalityw=1,haswell=true"
        String gridResourceLimits = ProcessorHelper.getGridJobResourceLimits(jobResources);
        if (StringUtils.isNotBlank(gridResourceLimits)) {
            nativeSpecBuilder.append("-l ").append(gridResourceLimits).append(' ');
        }
        return nativeSpecBuilder.toString();
    }

}
