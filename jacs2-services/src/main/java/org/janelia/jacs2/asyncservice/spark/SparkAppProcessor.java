package org.janelia.jacs2.asyncservice.spark;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import org.janelia.jacs2.asyncservice.common.ComputationException;
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
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;

/**
 * Full cycle spark app processor that starts a spark cluster, runs the specified app and shuts down the cluster.
 */
@Named("sparkAppProcessor")
public class SparkAppProcessor extends AbstractSparkProcessor<Void> {

    static class SparkAppArgs extends SparkArgs {
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
                      @IntPropertyValue(name = "service.spark.defaultNumNodes", defaultValue = 2) Integer defaultNumNodes,
                      @IntPropertyValue(name = "service.spark.defaultMinRequiredWorkers", defaultValue = 1) Integer defaultMinRequiredWorkers,
                      Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, clusterLauncher, defaultNumNodes, defaultMinRequiredWorkers, logger);
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

        DataHolder<SparkCluster> runningClusterState = new DataHolder<>();
        int requestedSparkNodes = getRequestedNodes(jacsServiceData.getResources());
        jacsServiceDataPersistence.addServiceEvent(
                jacsServiceData,
                JacsServiceData.createServiceEvent(JacsServiceEventTypes.START_PROCESS,
                        String.format("Starting a spark cluster with %d nodes", requestedSparkNodes)));
        return sparkClusterLauncher.startCluster(
                requestedSparkNodes,
                getMinRequiredWorkers(jacsServiceData.getResources()),
                serviceWorkingFolder.getServiceFolder(),
                accounting.getComputeAccount(jacsServiceData),
                getSparkDriverMemory(jacsServiceData.getResources()),
                getSparkExecutorMemory(jacsServiceData.getResources()),
                getSparkLogConfigFile(jacsServiceData.getResources()))
                .thenCompose(sparkCluster -> {
                    runningClusterState.setData(sparkCluster);
                    jacsServiceDataPersistence.addServiceEvent(
                            jacsServiceData,
                            JacsServiceData.createServiceEvent(JacsServiceEventTypes.CLUSTER_SUBMIT,
                                    String.format("Running app %s:%s using spark job on %s (%s)",
                                            args.appLocation,
                                            args.appEntryPoint,
                                            sparkCluster.getMasterURI(),
                                            sparkCluster.getMasterJobId())));
                    // the computation completes when the app completes
                    return sparkCluster.runApp(
                            args.appLocation,
                            args.appEntryPoint,
                            getDefaultParallelism(jacsServiceData.getResources()),
                            jacsServiceData.getOutputPath(),
                            jacsServiceData.getErrorPath(),
                            getSparkAppIntervalCheckInMillis(jacsServiceData.getResources()),
                            getSparkAppTimeoutInMillis(jacsServiceData.getResources()),
                            args.concatArgs(ImmutableList.of(args.appArgs, args.getRemainingArgs()))
                    );
                })
                .whenComplete(((sparkApp, exc) -> {
                    if (runningClusterState.isPresent()) {
                        jacsServiceDataPersistence.addServiceEvent(
                                jacsServiceData,
                                JacsServiceData.createServiceEvent(JacsServiceEventTypes.CLUSTER_STOP_JOB,
                                        String.format("Stop spark cluster on %s (%s)",
                                                runningClusterState.getData().getMasterURI(),
                                                runningClusterState.getData().getMasterJobId())));
                        runningClusterState.getData().stopCluster();
                    }
                }))
                .thenApply(sparkApp -> {
                    if (sparkApp.isError()) {
                        logger.error("Spark application error");
                        jacsServiceDataPersistence.updateServiceState(
                                jacsServiceData,
                                JacsServiceState.ERROR,
                                JacsServiceData.createServiceEvent(JacsServiceEventTypes.FAILED, sparkApp.getErrorMessage()));
                        throw new ComputationException(jacsServiceData, sparkApp.getErrorMessage());
                    } else {
                        return new JacsServiceResult<>(jacsServiceData);
                    }
                })
                ;
    }
}
