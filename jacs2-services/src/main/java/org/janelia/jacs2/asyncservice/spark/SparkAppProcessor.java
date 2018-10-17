package org.janelia.jacs2.asyncservice.spark;

import com.google.common.collect.ImmutableList;
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
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;

/**
 * Full cycle spark app processor that starts a spark cluster, runs the specified app and shuts down the cluster.
 */
@Named("sparkAppProcessor")
public class SparkAppProcessor extends AbstractSparkProcessor<Void> {

    private final ComputeAccounting accounting;

    @Inject
    SparkAppProcessor(ServiceComputationFactory computationFactory,
                      JacsServiceDataPersistence jacsServiceDataPersistence,
                      @StrPropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                      LSFSparkClusterLauncher clusterLauncher,
                      ComputeAccounting accounting,
                      @IntPropertyValue(name = "service.spark.defaultNumNodes", defaultValue = 2) Integer defaultNumNodes,
                      Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, clusterLauncher, defaultNumNodes, logger);
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
        return sparkClusterLauncher.startCluster(
                getRequestedNodes(jacsServiceData.getResources()),
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
                                            sparkCluster.getJobId())));
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
                                                runningClusterState.getData().getJobId())));
                        runningClusterState.getData().stopCluster();
                    }
                }))
                .thenApply(sparkApp -> new JacsServiceResult<>(jacsServiceData))
                ;
    }
}
