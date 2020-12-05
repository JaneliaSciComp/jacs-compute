package org.janelia.jacs2.asyncservice.spark;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.cluster.ComputeAccounting;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.cdi.qualifier.StrPropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.JacsServiceState;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Named("runSparkApp")
public class SparkAppRunProcessor extends AbstractSparkProcessor<String> {

    static class SparkClusterAppArgs extends SparkClusterArgs {
        @Parameter(names = "-appLocation", description = "Spark application location", required = true)
        String appLocation;
        @Parameter(names = "-appEntryPoint", description = "Spark application entry point, i.e., java main class name")
        String appEntryPoint;
        @Parameter(names = "-appArgs", description = "Spark application arguments", splitter = ServiceArgSplitter.class)
        List<String> appArgs = new ArrayList<>();

        SparkClusterAppArgs() {
            super("Run spark application on an existing cluster");
        }
    }

    private final ComputeAccounting accounting;

    @Inject
    SparkAppRunProcessor(ServiceComputationFactory computationFactory,
                         JacsServiceDataPersistence jacsServiceDataPersistence,
                         @StrPropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                         LSFSparkClusterLauncher clusterLauncher,
                         ComputeAccounting accounting,
                         @StrPropertyValue(name = "service.spark.sparkHomeDir", defaultValue = "/misc/local/spark-2.3.1") String defaultSparkHomeDir,
                         @StrPropertyValue(name = "service.spark.driver.memory", defaultValue = "1g") String defaultSparkDriverMemory,
                         @IntPropertyValue(name = "service.spark.executor.cores", defaultValue = 5) int defaultCoresPerSparkExecutor,
                         @IntPropertyValue(name = "service.spark.executor.core.memoryGB", defaultValue = 15) int defaultSparkMemoryPerExecutorCoreInGB,
                         @IntPropertyValue(name = "service.spark.cluster.hard.duration.mins", defaultValue = 60) int sparkClusterHardDurationMins,
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
                sparkClusterHardDurationMins,
                defaultSparkLogConfigFile,
                hadoopHomeDir,
                logger);
        this.accounting = accounting;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SparkAppRunProcessor.class, new SparkClusterAppArgs());
    }

    @Override
    public ServiceComputation<JacsServiceResult<String>> process(JacsServiceData jacsServiceData) {
        SparkClusterAppArgs args = getArgs(jacsServiceData);
        // prepare service directories
        prepareSparkJobDirs(jacsServiceData);

        // prepare app resources
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
        // start a spark app on on existing cluster
        return computationFactory.newCompletedComputation(sparkClusterLauncher.createSparkCluster(
                args.getSparkMasterJobId(),
                args.getSparkWorkerJobId(),
                accounting.getComputeAccount(jacsServiceData)))
                .thenCompose(sparkCluster -> {
                    jacsServiceDataPersistence.addServiceEvent(
                            jacsServiceData,
                            JacsServiceData.createServiceEvent(JacsServiceEventTypes.START_PROCESS,
                                    String.format("Running app %s:%s using spark job on %s (%s, %s)",
                                            args.appLocation,
                                            args.appEntryPoint,
                                            sparkCluster.getSparkClusterInfo().getMasterURI(),
                                            sparkCluster.getSparkClusterInfo().getMasterJobId(),
                                            sparkCluster.getSparkClusterInfo().getWorkerJobId())));
                    // the computation completes when the app completes
                    return sparkCluster.runLocalProcessApp(
                            args.appLocation,
                            args.appEntryPoint,
                            args.concatArgs(ImmutableList.of(args.appArgs, args.getRemainingArgs())),
                            jacsServiceData.getOutputPath(),
                            jacsServiceData.getErrorPath(),
                            appResources);
                })
                .thenApply(sparkApp -> {
                    if (sparkApp.hasErrors()) {
                        logger.error("Spark application error");
                        jacsServiceDataPersistence.updateServiceState(
                                jacsServiceData,
                                JacsServiceState.ERROR,
                                JacsServiceData.createServiceEvent(JacsServiceEventTypes.FAILED, sparkApp.getErrors()));
                        throw new ComputationException(jacsServiceData, sparkApp.getErrors());
                    } else {
                        return updateServiceResult(jacsServiceData, sparkApp.getAppId());
                    }
                })
                ;
    }

    private SparkClusterAppArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SparkClusterAppArgs());
    }

}
