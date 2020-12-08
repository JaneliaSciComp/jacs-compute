package org.janelia.jacs2.asyncservice.spark;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public class SparkAppResourceHelper {
    public static class Builder {
        private final Map<String, String> appResources = new HashMap<>();

        public Builder sparkHome(String val) {
            return setString("sparkHome", val);
        }

        public Builder hadoopHome(String val) {
            return setString("hadoopHome", val);
        }

        public Builder sparkParallelism(Integer val) {
            return setInteger("sparkParallelism", val);
        }

        public Builder sparkDriverCores(Integer val) {
            return setInteger("sparkDriverCores", val);
        }

        public Builder sparkDriverMemory(String val) {
            return setString("sparkDriverMemory", val);
        }

        public Builder sparkExecutorMemory(String val) {
            return setString("sparkExecutorMemory", val);
        }

        public Builder sparkAppStackSize(String val) {
            return setString("sparkAppStackSize", val);
        }

        public Builder sparkWorkers(Integer val) {
            return setInteger("sparkWorkers", val);
        }

        public Builder minSparkWorkers(Integer val) {
            return setInteger("minSparkWorkers", val);
        }

        public Builder sparkWorkerCores(Integer val) {
            return setInteger("sparkWorkerCores", val);
        }

        public Builder sparkWorkerMemoryPerCoreInGB(Integer val) {
            return setInteger("sparkWorkerMemoryPerCoreInGB", val);
        }

        public Builder sparkAppIntervalCheckInMillis(Integer val) {
            return setInteger("sparkAppIntervalCheckInMillis", val);
        }

        public Builder sparkAppTimeoutInMillis(Long val) {
            return setLong("sparkAppTimeoutInMillis", val);
        }

        public Builder sparkLogConfigFile(String val) {
            return setString("sparkLogConfigFile", val);
        }

        public Builder addAll(Map<String, String> appResources) {
            this.appResources.putAll(appResources);
            return this;
        }

        private Builder setString(String attr, String val) {
            if (StringUtils.isNotBlank(val)) appResources.put(attr, val);
            return this;
        }

        private Builder setInteger(String attr, Integer val) {
            if (val != null) appResources.put(attr, val.toString());
            return this;
        }

        private Builder setLong(String attr, Long val) {
            if (val != null) appResources.put(attr, val.toString());
            return this;
        }

        public Map<String, String> build() {
            return appResources;
        }
    }

    public static Builder sparkAppResourceBuilder() {
        return new Builder();
    }

    static String getSparkHome(Map<String, String> sparkResources) {
        return sparkResources.get("sparkHome");
    }

    static String getHadoopHome(Map<String, String> sparkResources) {
        return sparkResources.get("hadoopHome");
    }

    static int getSparkWorkers(Map<String, String> sparkResources) {
        String sparkWorkers = StringUtils.defaultIfBlank(
                sparkResources.getOrDefault("sparkWorkers", getSparkNodes(sparkResources)), "1");
        return Math.max(1, Integer.parseInt(sparkWorkers));
    }

    private static String getSparkNodes(Map<String, String> sparkResources) {
        // this is the legacy mechanism of getting the workers where the entire node was allocated for a spark job
        String sparkNodes = sparkResources.get("sparkNumNodes");
        if (StringUtils.isNotBlank(sparkNodes)) {
            int nSparkNodes = Integer.parseInt(sparkNodes);
            // the legacy spark app allocated 5 slots on each node per worker, therefore each node could have up to 6 spark workers
            return String.valueOf(nSparkNodes * 6);
        } else {
            return null;
        }
    }

    static int getSparkWorkerCores(Map<String, String> sparkResources) {
        String sparkWorkerCores = StringUtils.defaultIfBlank(sparkResources.get("sparkWorkerCores"), "1");
        return Math.max(1, Integer.parseInt(sparkWorkerCores));
    }

    static int getSparkWorkerMemoryPerCoreInGB(Map<String, String> sparkResources) {
        String sparkWorkerMemoryPerCoreInGB = StringUtils.defaultIfBlank(sparkResources.get("sparkWorkerMemoryPerCoreInGB"), "0");
        return Math.max(0, Integer.parseInt(sparkWorkerMemoryPerCoreInGB));
    }

    static int getMinRequiredWorkers(Map<String, String> sparkResources) {
        String minSparkWorkersValue = StringUtils.defaultIfBlank(sparkResources.get("minSparkWorkers"), "0");
        return Math.max(0, Integer.parseInt(minSparkWorkersValue));
    }

    static int getSparkParallelism(Map<String, String> serviceResources) {
        String defaultParallelism = StringUtils.defaultIfBlank(serviceResources.get("sparkParallelism"), "0");
        return Math.max(0, Integer.parseInt(defaultParallelism));
    }

    static int getSparkDriverCores(Map<String, String> sparkResources) {
        String sparkDriverCores = StringUtils.defaultIfBlank(sparkResources.get("sparkDriverCores"), "1");
        return Math.max(1, Integer.parseInt(sparkDriverCores));
    }

    static String getSparkDriverMemory(Map<String, String> sparkResources) {
        return sparkResources.get("sparkDriverMemory");
    }

    static String getSparkExecutorMemory(Map<String, String> sparkResources) {
        return sparkResources.get("sparkExecutorMemory");
    }

    static String getSparkAppStackSize(Map<String, String> sparkResources) {
        return sparkResources.get("sparkAppStackSize");
    }

    static Long getSparkAppIntervalCheckInMillis(Map<String, String> sparkResources) {
        String intervalCheck = sparkResources.get("sparkAppIntervalCheckInMillis");
        if (StringUtils.isNotBlank(intervalCheck)) {
            return Long.valueOf(intervalCheck.trim());
        } else {
            return null;
        }
    }

    static Long getSparkAppTimeoutInMillis(Map<String, String> sparkResources) {
        String timeout = sparkResources.get("sparkAppTimeoutInMillis");
        if (StringUtils.isNotBlank(timeout)) {
            return Long.valueOf(timeout.trim());
        } else {
            return null;
        }
    }

    static int getSparkAppTimeoutInMin(Map<String, String> sparkResources) {
        Long timeoutInMillis = getSparkAppTimeoutInMillis(sparkResources);
        if (timeoutInMillis != null && timeoutInMillis > 0L) {
            return (int) Duration.ofMillis(timeoutInMillis).toMinutes();
        } else
            return 0;
    }

    static String getSparkLogConfigFile(Map<String, String> sparkResources) {
        return sparkResources.get("sparkLogConfigFile");
    }

}
