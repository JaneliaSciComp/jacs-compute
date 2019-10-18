package org.janelia.jacs2.asyncservice.spark;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.StrPropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.slf4j.Logger;

import javax.inject.Inject;
import java.time.Duration;
import java.util.Map;

abstract public class AbstractSparkProcessor<R> extends AbstractServiceProcessor<R> {

    protected final LSFSparkClusterLauncher sparkClusterLauncher;
    protected final int defaultNumNodes;
    protected final int defaultMinRequiredWorkers;

    protected AbstractSparkProcessor(ServiceComputationFactory computationFactory,
                                     JacsServiceDataPersistence jacsServiceDataPersistence,
                                     String defaultWorkingDir,
                                     LSFSparkClusterLauncher sparkClusterLauncher,
                                     Integer defaultNumNodes,
                                     Integer defaultMinRequiredWorkers,
                                     Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.sparkClusterLauncher = sparkClusterLauncher;
        this.defaultNumNodes = defaultNumNodes == null || defaultNumNodes <= 0 ? 1 : defaultNumNodes;
        this.defaultMinRequiredWorkers = defaultMinRequiredWorkers == null || defaultMinRequiredWorkers < 0 ? 0 : defaultMinRequiredWorkers;
    }

    int getRequestedNodes(Map<String, String> serviceResources) {
        String requestedNodes = StringUtils.defaultIfBlank(serviceResources.get("sparkNumNodes"), "1");
        int numNodes = Integer.parseInt(requestedNodes);
        return numNodes <= 0 ? defaultNumNodes : numNodes;
    }

    int getMinRequiredWorkers(Map<String, String> serviceResources) {
        String minSparkWorkersValue = StringUtils.defaultIfBlank(serviceResources.get("minSparkWorkers"), "0");
        int minSparkWorkers = Integer.parseInt(minSparkWorkersValue);
        return minSparkWorkers <= 0 ? defaultMinRequiredWorkers : minSparkWorkers;
    }

    int getDefaultParallelism(Map<String, String> serviceResources) {
        String defaultParallelism = StringUtils.defaultIfBlank(serviceResources.get("sparkDefaultParallelism"), "0");
        int parallelism = Integer.parseInt(defaultParallelism);
        return parallelism <= 0 ? 0 : parallelism;
    }

    protected JacsServiceFolder prepareSparkJobDirs(JacsServiceData jacsServiceData) {
        JacsServiceFolder serviceWorkingFolder = getWorkingDirectory(jacsServiceData);
        updateOutputAndErrorPaths(jacsServiceData);
        prepareDir(serviceWorkingFolder.getServiceFolder().toString());
        prepareDir(jacsServiceData.getOutputPath());
        prepareDir(jacsServiceData.getErrorPath());
        return serviceWorkingFolder;
    }

    protected String getSparkDriverMemory(Map<String, String> serviceResources) {
        return serviceResources.get("sparkDriverMemory");
    }

    protected String getSparkExecutorMemory(Map<String, String> serviceResources) {
        return serviceResources.get("sparkExecutorMemory");
    }

    String getSparkAppStackSize(Map<String, String> serviceResources) {
        return serviceResources.get("sparkAppStackSize");
    }

    Long getSparkAppIntervalCheckInMillis(Map<String, String> serviceResources) {
        String intervalCheck = serviceResources.get("sparkAppIntervalCheckInMillis");
        if (StringUtils.isNotBlank(intervalCheck)) {
            return Long.valueOf(intervalCheck.trim());
        } else {
            return null;
        }
    }

    Long getSparkAppTimeoutInMillis(Map<String, String> serviceResources) {
        String timeout = serviceResources.get("sparkAppTimeoutInMillis");
        if (StringUtils.isNotBlank(timeout)) {
            return Long.valueOf(timeout.trim());
        } else {
            return null;
        }
    }

    int serviceTimeoutInMins(JacsServiceData jacsServiceData) {
        Long sparkAppTimeoutInMillis = getSparkAppTimeoutInMillis(jacsServiceData.getResources());
        if (sparkAppTimeoutInMillis != null && sparkAppTimeoutInMillis > 0) {
            return (int) (Duration.ofMillis(sparkAppTimeoutInMillis).toMinutes() + 1);
        } else if (jacsServiceData.timeoutInMins() > 0) {
            return jacsServiceData.timeoutInMins() + 1;
        } else {
            return -1;
        }
    }

    protected String getSparkLogConfigFile(Map<String, String> serviceResources) {
        return serviceResources.get("sparkLogConfigFile");
    }
}
