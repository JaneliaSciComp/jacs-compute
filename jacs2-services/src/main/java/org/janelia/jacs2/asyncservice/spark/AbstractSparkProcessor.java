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
import java.util.Map;

abstract public class AbstractSparkProcessor<R> extends AbstractServiceProcessor<R> {

    protected final LSFSparkClusterLauncher sparkClusterLauncher;
    protected final int defaultNumNodes;

    @Inject
    protected AbstractSparkProcessor(ServiceComputationFactory computationFactory,
                                     JacsServiceDataPersistence jacsServiceDataPersistence,
                                     @StrPropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                     LSFSparkClusterLauncher sparkClusterLauncher,
                                     @IntPropertyValue(name = "service.spark.defaultNumNodes", defaultValue = 2) Integer defaultNumNodes,
                                     Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.sparkClusterLauncher = sparkClusterLauncher;
        this.defaultNumNodes = defaultNumNodes <= 0 ? 1 : defaultNumNodes;
    }


    int getRequestedNodes(Map<String, String> serviceResources) {
        String requestedNodes = StringUtils.defaultIfBlank(serviceResources.get("spark.numNodes"), "1");
        int numNodes = Integer.parseInt(requestedNodes);
        return numNodes <= 0 ? defaultNumNodes : numNodes;
    }

    int getDefaultParallelism(Map<String, String> serviceResources) {
        String defaultParallelism = StringUtils.defaultIfBlank(serviceResources.get("spark.defaultParallelism"), "0");
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
        return serviceResources.get("spark.driverMemory");
    }

    protected String getSparkExecutorMemory(Map<String, String> serviceResources) {
        return serviceResources.get("spark.executorMemory");
    }

    protected int getSparkExecutorCores(Map<String, String> serviceResources) {
        String sparkExecutorCores = StringUtils.defaultIfBlank(serviceResources.get("spark.executorCores"), "0");
        int executorCores = Integer.parseInt(sparkExecutorCores);
        return executorCores <= 0 ? 0 : executorCores;
    }

    Long getSparkAppIntervalCheckInMillis(Map<String, String> serviceResources) {
        String intervalCheck = serviceResources.get("spark.appIntervalCheckInMillis");
        if (StringUtils.isNotBlank(intervalCheck)) {
            return Long.valueOf(intervalCheck.trim());
        } else {
            return null;
        }
    }

    Long getSparkAppTimeoutInMillis(Map<String, String> serviceResources) {
        String timeout = serviceResources.get("spark.appTimeoutInMillis");
        if (StringUtils.isNotBlank(timeout)) {
            return Long.valueOf(timeout.trim());
        } else {
            return null;
        }
    }

    protected String getSparkLogConfigFile(Map<String, String> serviceResources) {
        return serviceResources.get("spark.logConfigFile");
    }
}
