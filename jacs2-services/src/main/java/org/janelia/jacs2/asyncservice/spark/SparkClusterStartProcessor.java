package org.janelia.jacs2.asyncservice.spark;

import java.nio.file.Paths;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Named;

import com.fasterxml.jackson.core.type.TypeReference;

import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.cluster.ComputeAccounting;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.StrPropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

@Named("startSparkCluster")
public class SparkClusterStartProcessor extends AbstractSparkProcessor<SparkClusterInfo> {

    static class StartSparkJobArgs extends SparkArgs {
        StartSparkJobArgs() {
            super("Start spark cluster");
        }
    }

    private final ComputeAccounting accounting;

    @Inject
    SparkClusterStartProcessor(ServiceComputationFactory computationFactory,
                               JacsServiceDataPersistence jacsServiceDataPersistence,
                               @StrPropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                               LSFSparkClusterLauncher clusterLauncher,
                               ComputeAccounting accounting,
                               @StrPropertyValue(name = "service.spark.sparkHomeDir") String defaultSparkHomeDir,
                               @StrPropertyValue(name = "service.spark.driver.memory", defaultValue = "1g") String defaultSparkDriverMemory,
                               @IntPropertyValue(name = "service.spark.executor.cores", defaultValue = 5) int defaultCoresPerSparkExecutor,
                               @IntPropertyValue(name = "service.spark.executor.core.memoryGB", defaultValue = 15) int defaultSparkMemoryPerExecutorCoreInGB,
                               @StrPropertyValue(name = "service.spark.log4jconfig.filepath", defaultValue = "") String defaultSparkLogConfigFile,
                               @StrPropertyValue(name = "hadoop.homeDir") String hadoopHomeDir,
                               Logger logger) {
        super(computationFactory,
                jacsServiceDataPersistence,
                defaultWorkingDir,
                clusterLauncher,
                defaultSparkHomeDir,
                defaultSparkDriverMemory,
                defaultCoresPerSparkExecutor,
                defaultSparkMemoryPerExecutorCoreInGB,
                -1,
                defaultSparkLogConfigFile,
                hadoopHomeDir,
                logger);
        this.accounting = accounting;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SparkClusterStartProcessor.class, new StartSparkJobArgs());
    }

    @Override
    public ServiceResultHandler<SparkClusterInfo> getResultHandler() {
        return new AbstractAnyServiceResultHandler<SparkClusterInfo>() {
            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                return areAllDependenciesDone(jacsServiceData);
            }

            @Override
            public SparkClusterInfo getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<SparkClusterInfo>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<SparkClusterInfo>> process(JacsServiceData jacsServiceData) {
        // prepare service directories
        JacsServiceFolder serviceWorkingFolder = prepareSparkJobDirs(jacsServiceData);

        return startCluster(jacsServiceData, serviceWorkingFolder)
                .thenApply(sparkCluster -> {
                            jacsServiceDataPersistence.addServiceEvent(
                                    jacsServiceData,
                                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.CLUSTER_SUBMIT,
                                            String.format("Started spark cluster %s (%s, %s)",
                                                    sparkCluster.getSparkClusterInfo().getMasterURI(),
                                                    sparkCluster.getSparkClusterInfo().getMasterJobId(),
                                                    sparkCluster.getSparkClusterInfo().getWorkerJobId())));
                            return updateServiceResult(
                                    jacsServiceData,
                                    sparkCluster.getSparkClusterInfo()
                            );
                })
                ;
    }

    private ServiceComputation<LSFSparkCluster> startCluster(JacsServiceData jacsServiceData, JacsServiceFolder serviceWorkingFolder) {
        Map<String, String> appResources = SparkAppResourceHelper.sparkAppResourceBuilder()
                .sparkHome(getDefaultSparkHome())
                .sparkDriverMemory(getDefaultSparkDriverMemory())
                .sparkWorkerCores(getDefaultCoresPerSparkExecutor())
                .sparkWorkerMemoryPerCoreInGB(getDefaultSparkMemoryPerExecutorCoreInGB())
                .sparkAppTimeoutInMillis(getSparkClusterHardDurationMins() * 60L * 1000L)
                .sparkLogConfigFile(getDefaultSparkLogConfigFile())
                .hadoopHome(getHadoopHome())
                .addAll(jacsServiceData.getResources())
                .build();
        return sparkClusterLauncher.startCluster(
                "sparkjacs",
                SparkAppResourceHelper.getSparkHome(appResources),
                SparkAppResourceHelper.getSparkWorkers(appResources),
                SparkAppResourceHelper.getSparkWorkerCores(appResources),
                SparkAppResourceHelper.getMinRequiredWorkers(appResources),
                serviceWorkingFolder.getServiceFolder(),
                Paths.get(jacsServiceData.getOutputPath()),
                Paths.get(jacsServiceData.getErrorPath()),
                accounting.getComputeAccount(jacsServiceData),
                serviceTimeoutInMins(jacsServiceData, appResources)
        );
    }
}
