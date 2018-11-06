package org.janelia.jacs2.asyncservice.common;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class ProcessorHelper {

    private static final String DEFAULT_CPU_TYPE = "haswell";

    public static int getProcessingSlots(Map<String, String> serviceResources) {
        int nProcessingSlots = getRequiredSlots(serviceResources);
        nProcessingSlots = Math.max(nProcessingSlots, calculateRequiredSlotsBasedOnMem(serviceResources));
        return nProcessingSlots > 0 ? nProcessingSlots : 1;
    }

    public static int getRequiredSlots(Map<String, String> serviceResources) {
        String requiredSlots = StringUtils.defaultIfBlank(serviceResources.get("nSlots"), "0");
        int nSlots = Integer.parseInt(requiredSlots);
        return nSlots <= 0 ? 0 : nSlots;
    }

    public static int setRequiredSlots(Map<String, String> serviceResources, int slots) {
        int currentRequiredSlots = getRequiredSlots(serviceResources);
        int requiredSlots = Math.max(currentRequiredSlots, slots);
        serviceResources.put("nSlots", String.valueOf(requiredSlots));
        return requiredSlots;
    }

    public static String getCPUType(Map<String, String> serviceResources) {
        String cpuType = serviceResources.get("cpuType");
        return cpuType != null ? cpuType.toLowerCase() : null;
    }

    public static String setCPUType(Map<String, String> serviceResources, String cpuType) {
        serviceResources.put("cpuType", cpuType);
        return cpuType;
    }

    public static int getRequiredMemoryInGB(Map<String, String> serviceResources) {
        String requiredMemory = StringUtils.defaultIfBlank(serviceResources.get("memInGB"), "0");
        int requiredMemoryInGB = Integer.parseInt(requiredMemory);
        return requiredMemoryInGB <= 0 ? 0 : requiredMemoryInGB;
    }

    public static int setRequiredMemoryInGB(Map<String, String> serviceResources, int mem) {
        int currentRequiredMemory = getRequiredMemoryInGB(serviceResources);
        int requiredMemoryInGB = Math.max(currentRequiredMemory, mem);
        serviceResources.put("memInGB", String.valueOf(requiredMemoryInGB));
        return requiredMemoryInGB;
    }

    private static int calculateRequiredSlotsBasedOnMem(Map<String, String> serviceResources) {
        int mem = getRequiredMemoryInGB(serviceResources);
        if (mem <= 0) {
            return 0;
        }
        double memPerSlotInGB = 15.5; // all nodes (irrespective of the architecture now have 15G per slot)
        return (int) Math.ceil(mem / memPerSlotInGB);
    }

    public static String getGridBillingAccount(Map<String, String> serviceResources) {
        return serviceResources.get("gridAccountId");
    }

    public static long getSoftJobDurationLimitInSeconds(Map<String, String> serviceResources) {
        String jobDuration = StringUtils.defaultIfBlank(serviceResources.get("softGridJobDurationInSeconds"), "-1");
        return Long.parseLong(jobDuration);
    }

    public static long setSoftJobDurationLimitInSeconds(Map<String, String> serviceResources, long limit) {
        serviceResources.put("softGridJobDurationInSeconds", ""+limit);
        return limit;
    }

    public static long getHardJobDurationLimitInSeconds(Map<String, String> serviceResources) {
        String jobDuration = StringUtils.defaultIfBlank(serviceResources.get("hardGridJobDurationInSeconds"), "-1");
        return Long.parseLong(jobDuration);
    }

    public static long setHardJobDurationLimitInSeconds(Map<String, String> serviceResources, long limit) {
        serviceResources.put("hardGridJobDurationInSeconds", ""+limit);
        return limit;
    }

    public static String getGridJobResourceLimits(Map<String, String> serviceResources) {
        return serviceResources.get("gridResourceLimits");
    }
}
