package org.janelia.jacs2.asyncservice.common;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class ProcessorHelper {

    public static int getProcessingSlots(Map<String, String> jobResources) {
        String processingSlots = StringUtils.defaultIfBlank(jobResources.get("nSlots"), "1");
        int nProcessingSlots = Integer.parseInt(processingSlots);
        return nProcessingSlots > 0 ? nProcessingSlots : 1;
    }
}
