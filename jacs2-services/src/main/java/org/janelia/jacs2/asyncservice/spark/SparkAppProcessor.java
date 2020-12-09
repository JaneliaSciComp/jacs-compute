package org.janelia.jacs2.asyncservice.spark;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Named;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;

import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ContinuationCond;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.cluster.ComputeAccounting;
import org.janelia.jacs2.asyncservice.utils.DataHolder;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.StrPropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.JacsServiceState;
import org.janelia.model.service.ProcessingLocation;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

/**
 * Full cycle spark app processor that starts a spark cluster, runs the specified app and shuts down the cluster.
 */
@Named("sparkAppProcessor")
public class SparkAppProcessor extends AbstractSparkProcessor<Void> {

    static class SparkAppArgs extends SparkArgs {
        @Parameter(names = "-appName", description = "Spark application name")
        String appName = "sparkjacs";
        @Parameter(names = "-appLocation", description = "Spark application location", required = true)
        String appLocation;
        @Parameter(names = "-appEntryPoint", description = "Spark application entry point, i.e., java main class name")
        String appEntryPoint;
        @Parameter(names = "-appArgs", description = "Spark application arguments", splitter = ServiceArgSplitter.class)
        List<String> appArgs = new ArrayList<>();

        SparkAppArgs() {
            super("Spark application processor");
        }
    }

    private final ComputeAccounting accounting;

    @Inject
    SparkAppProcessor(ServiceComputationFactory computationFactory,
                      JacsServiceDataPersistence jacsServiceDataPersistence,
                      @StrPropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                      LSFSparkClusterLauncher clusterLauncher,
                      ComputeAccounting accounting,
                      @StrPropertyValue(name = "service.spark.sparkHomeDir") String defaultSparkHomeDir,
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
        return ServiceArgs.getMetadata(SparkAppProcessor.class, new SparkAppArgs());
    }

    private SparkAppArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SparkAppArgs());
    }

    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        SparkAppArgs args = getArgs(jacsServiceData);

        // prepare spark job directories
        JacsServiceFolder serviceWorkingFolder = prepareSparkJobDirs(jacsServiceData);

        DataHolder<LSFSparkCluster> runningClusterState = new DataHolder<>();
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
        // create the spark cluster and start the app
        int requestedSparkWorkers = SparkAppResourceHelper.getSparkWorkers(appResources);
        jacsServiceDataPersistence.addServiceEvent(
                jacsServiceData,
                JacsServiceData.createServiceEvent(JacsServiceEventTypes.START_PROCESS,
                        String.format("Starting a spark cluster with %d nodes", requestedSparkWorkers)));
        String billingInfo = accounting.getComputeAccount(jacsServiceData);
        return sparkClusterLauncher.startCluster(
                args.appName,
                SparkAppResourceHelper.getSparkHome(appResources),
                SparkAppResourceHelper.getSparkWorkers(appResources),
                SparkAppResourceHelper.getSparkWorkerCores(appResources),
                SparkAppResourceHelper.getMinRequiredWorkers(appResources),
                serviceWorkingFolder.getServiceFolder(JacsServiceFolder.SERVICE_CONFIG_DIR),
                Paths.get(jacsServiceData.getOutputPath()),
                Paths.get(jacsServiceData.getErrorPath()),
                billingInfo,
                serviceTimeoutInMins(jacsServiceData, appResources))
                .thenCompose(sparkCluster -> {
                    logger.info("Started spark cluster {}", sparkCluster.getSparkClusterInfo());
                    runningClusterState.setData(sparkCluster);
                    jacsServiceDataPersistence.addServiceEvent(
                            jacsServiceData,
                            JacsServiceData.createServiceEvent(JacsServiceEventTypes.CLUSTER_SUBMIT,
                                    String.format("Started spark cluster %s (%s, %s) for running %s:%s",
                                            sparkCluster.getSparkClusterInfo().getMasterURI(),
                                            sparkCluster.getSparkClusterInfo().getMasterJobId(),
                                            sparkCluster.getSparkClusterInfo().getWorkerJobId(),
                                            args.appLocation,
                                            args.appEntryPoint)));
                    SparkDriverRunner<? extends SparkApp> sparkDriverRunner;
                    if (jacsServiceData.getProcessingLocation() == ProcessingLocation.LSF_JAVA) {
                        sparkDriverRunner = sparkClusterLauncher.getLSFDriverRunner(billingInfo);
                    } else {
                        sparkDriverRunner = sparkClusterLauncher.getLocalDriverRunner();
                    }
                    // the computation completes when the app completes
                    return computationFactory.newCompletedComputation(
                            sparkDriverRunner.startSparkApp(
                                    args.appName,
                                    sparkCluster.getSparkClusterInfo(),
                                    args.appLocation,
                                    args.appEntryPoint,
                                    ServiceArgs.concatArgs(ImmutableList.of(args.appArgs, args.getRemainingArgs())),
                                    jacsServiceData.getOutputPath(),
                                    jacsServiceData.getErrorPath(),
                                    appResources))
                            .thenSuspendUntil(app -> new ContinuationCond.Cond<>(app, app.isDone()),
                                    SparkAppResourceHelper.getSparkAppIntervalCheckInMillis(appResources),
                                    SparkAppResourceHelper.getSparkAppTimeoutInMillis(appResources));
                })
                .whenComplete(((sparkApp, exc) -> {
                    if (runningClusterState.isPresent()) {
                        jacsServiceDataPersistence.addServiceEvent(
                                jacsServiceData,
                                JacsServiceData.createServiceEvent(JacsServiceEventTypes.CLUSTER_STOP_JOB,
                                        String.format("Stop spark cluster on %s (%s, %s)",
                                                runningClusterState.getData().getSparkClusterInfo().getMasterURI(),
                                                runningClusterState.getData().getSparkClusterInfo().getMasterJobId(),
                                                runningClusterState.getData().getSparkClusterInfo().getWorkerJobId())));
                        if (sparkApp != null) sparkApp.kill(); // terminate the app just in case it is still running
                        if (exc != null) {
                            logger.error("Spark processing error encountered", exc);
                            jacsServiceDataPersistence.updateServiceState(
                                    jacsServiceData,
                                    JacsServiceState.ERROR,
                                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.FAILED, exc.toString()));
                            throw new ComputationException(jacsServiceData, exc.toString());
                        } else if (sparkApp.hasErrors()) {
                            logger.error("Spark application error: {}", sparkApp.getErrors());
                            jacsServiceDataPersistence.updateServiceState(
                                    jacsServiceData,
                                    JacsServiceState.ERROR,
                                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.FAILED, sparkApp.getErrors()));
                            throw new ComputationException(jacsServiceData, sparkApp.getErrors());
                        }
                        runningClusterState.getData().stopCluster();
                    }
                }))
                .thenApply(sparkApp -> new JacsServiceResult<>(jacsServiceData))
                ;
    }
}
