package org.janelia.jacs2.asyncservice.spark;

import java.util.Map;

import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.slf4j.Logger;

abstract public class AbstractSparkProcessor<R> extends AbstractServiceProcessor<R> {

    protected final LSFSparkClusterLauncher sparkClusterLauncher;
    private final String defaultSparkHome;
    private final String defaultSparkDriverMemory;
    private final int defaultCoresPerSparkExecutor;
    private final int defaultSparkMemoryPerExecutorCoreInGB;
    private final int sparkClusterHardDurationMins;
    private final String defaultSparkLogConfigFile;
    private final String hadoopHome;

    protected AbstractSparkProcessor(ServiceComputationFactory computationFactory,
                                     JacsServiceDataPersistence jacsServiceDataPersistence,
                                     String defaultWorkingDir,
                                     LSFSparkClusterLauncher sparkClusterLauncher,
                                     String defaultSparkHome,
                                     String defaultSparkDriverMemory,
                                     int defaultCoresPerSparkExecutor,
                                     int defaultSparkMemoryPerExecutorCoreInGB,
                                     int sparkClusterHardDurationMins,
                                     String defaultSparkLogConfigFile,
                                     String hadoopHome,
                                     Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.sparkClusterLauncher = sparkClusterLauncher;
        this.defaultSparkHome = defaultSparkHome;
        this.defaultSparkDriverMemory = defaultSparkDriverMemory;
        this.defaultCoresPerSparkExecutor = defaultCoresPerSparkExecutor;
        this.defaultSparkMemoryPerExecutorCoreInGB = defaultSparkMemoryPerExecutorCoreInGB;
        this.sparkClusterHardDurationMins = sparkClusterHardDurationMins;
        this.defaultSparkLogConfigFile = defaultSparkLogConfigFile;
        this.hadoopHome = hadoopHome;
    }

    protected String getDefaultSparkHome() {
        return defaultSparkHome;
    }

    protected String getDefaultSparkDriverMemory() {
        return defaultSparkDriverMemory;
    }

    protected int getDefaultCoresPerSparkExecutor() {
        return defaultCoresPerSparkExecutor;
    }

    protected int getDefaultSparkMemoryPerExecutorCoreInGB() {
        return defaultSparkMemoryPerExecutorCoreInGB;
    }

    protected int getSparkClusterHardDurationMins() {
        return sparkClusterHardDurationMins;
    }

    protected String getDefaultSparkLogConfigFile() {
        return defaultSparkLogConfigFile;
    }

    protected String getHadoopHome() {
        return hadoopHome;
    }

    protected JacsServiceFolder prepareSparkJobDirs(JacsServiceData jacsServiceData) {
        JacsServiceFolder serviceWorkingFolder = getWorkingDirectory(jacsServiceData);
        updateOutputAndErrorPaths(jacsServiceData);
        prepareDir(serviceWorkingFolder.getServiceFolder().toString());
        prepareDir(jacsServiceData.getOutputPath());
        prepareDir(jacsServiceData.getErrorPath());
        return serviceWorkingFolder;
    }

    int serviceTimeoutInMins(JacsServiceData jacsServiceData, Map<String, String> appResources) {
        int sparkAppTimeoutInMin = SparkAppResourceHelper.getSparkAppTimeoutInMin(appResources);
        if (sparkAppTimeoutInMin > 0) {
            return sparkAppTimeoutInMin;
        } else if (jacsServiceData.timeoutInMins() > 0) {
            return jacsServiceData.timeoutInMins();
        } else {
            return -1;
        }
    }

}
