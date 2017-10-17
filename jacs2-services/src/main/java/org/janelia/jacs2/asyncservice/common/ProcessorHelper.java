package org.janelia.jacs2.asyncservice.common;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class ProcessorHelper {

    private static final String DEFAULT_CPU_TYPE = "haswell";

    public static int getProcessingSlots(Map<String, String> jobResources) {
        int nProcessingSlots = getRequiredSlots(jobResources);
        nProcessingSlots = Math.max(nProcessingSlots, calculateRequiredSlotsBasedOnMem(jobResources));
        return nProcessingSlots > 0 ? nProcessingSlots : 1;
    }

    public static int getRequiredSlots(Map<String, String> jobResources) {
        String requiredSlots = StringUtils.defaultIfBlank(jobResources.get("nSlots"), "0");
        int nSlots = Integer.parseInt(requiredSlots);
        return nSlots <= 0 ? 0 : nSlots;
    }

    public static int setRequiredSlots(Map<String, String> jobResources, int slots) {
        int currentRequiredSlots = getRequiredSlots(jobResources);
        int requiredSlots = Math.max(currentRequiredSlots, slots);
        jobResources.put("nSlots", String.valueOf(requiredSlots));
        return requiredSlots;
    }

    public static String getCPUType(Map<String, String> jobResources) {
        String cpuType = jobResources.get("cpuType");
        return cpuType != null ? cpuType.toLowerCase() : null;
    }

    public static int getRequiredMemoryInGB(Map<String, String> jobResources) {
        String requiredMemory = StringUtils.defaultIfBlank(jobResources.get("memInGB"), "0");
        int requiredMemoryInGB = Integer.parseInt(requiredMemory);
        return requiredMemoryInGB <= 0 ? 0 : requiredMemoryInGB;
    }

    public static int setRequiredMemoryInGB(Map<String, String> jobResources, int mem) {
        int currentRequiredMemory = getRequiredMemoryInGB(jobResources);
        int requiredMemoryInGB = Math.max(currentRequiredMemory, mem);
        jobResources.put("memInGB", String.valueOf(requiredMemoryInGB));
        return requiredMemoryInGB;
    }

    private static int calculateRequiredSlotsBasedOnMem(Map<String, String> jobResources) {
        int mem = getRequiredMemoryInGB(jobResources);
        if (mem <= 0) {
            return 0;
        }
        String cpuType = StringUtils.defaultIfBlank(getCPUType(jobResources), DEFAULT_CPU_TYPE);
        double memPerSlotInGB;
        switch (cpuType) {
            case "broadwell":
                memPerSlotInGB = 15.5;
                break;
            default:
                memPerSlotInGB = 7.5;
                break;
        }
        return (int) Math.ceil(mem / memPerSlotInGB);
    }

    public static String getGridBillingAccount(Map<String, String> jobResources) {
        return jobResources.get("gridAccountId");
    }

    public static long getSoftJobDurationLimitInSeconds(Map<String, String> jobResources) {
        String jobDuration = StringUtils.defaultIfBlank(jobResources.get("softGridJobDurationInSeconds"), "-1");
        return Long.parseLong(jobDuration);
    }

    public static long getHardJobDurationLimitInSeconds(Map<String, String> jobResources) {
        String jobDuration = StringUtils.defaultIfBlank(jobResources.get("hardGridJobDurationInSeconds"), "-1");
        return Long.parseLong(jobDuration);
    }

    public static String getGridJobResourceLimits(Map<String, String> jobResources) {
        return jobResources.get("gridResourceLimits");
    }
}
