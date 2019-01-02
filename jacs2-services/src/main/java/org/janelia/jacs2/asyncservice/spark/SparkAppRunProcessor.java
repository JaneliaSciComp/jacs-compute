package org.janelia.jacs2.asyncservice.spark;

import com.google.common.collect.ImmutableList;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
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

@Named("runSparkApp")
public class SparkAppRunProcessor extends AbstractSparkProcessor<String> {

    @Inject
    SparkAppRunProcessor(ServiceComputationFactory computationFactory,
                         JacsServiceDataPersistence jacsServiceDataPersistence,
                         @StrPropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                         BatchLSFSparkClusterLauncher clusterLauncher,
                         @IntPropertyValue(name = "service.spark.defaultNumNodes", defaultValue = 2) Integer defaultNumNodes,
                         Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, clusterLauncher, defaultNumNodes, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SparkAppRunProcessor.class, new SparkAppArgs());
    }

    @Override
    public ServiceComputation<JacsServiceResult<String>> process(JacsServiceData jacsServiceData) {
        SparkAppArgs args = getArgs(jacsServiceData);
        // prepare service directories
        prepareSparkJobDirs(jacsServiceData);
        // start a spark app on on existing cluster
        return sparkClusterLauncher.createCluster(getSparkClusterJobId(args),
                sparkClusterLauncher.calculateDefaultParallelism(getRequestedNodes(jacsServiceData.getResources())),
                getSparkDriverMemory(jacsServiceData.getResources()),
                getSparkExecutorMemory(jacsServiceData.getResources()),
                getSparkLogConfigFile(jacsServiceData.getResources()))
                .thenCompose(sparkCluster -> {
                    jacsServiceDataPersistence.addServiceEvent(
                            jacsServiceData,
                            JacsServiceData.createServiceEvent(JacsServiceEventTypes.START_PROCESS,
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
                .thenApply(sparkApp -> {
                    if (sparkApp.isError()) {
                        logger.error("Spark application error");
                        jacsServiceDataPersistence.updateServiceState(
                                jacsServiceData,
                                JacsServiceState.ERROR,
                                JacsServiceData.createServiceEvent(JacsServiceEventTypes.FAILED, sparkApp.getErrorMessage()));
                        throw new ComputationException(jacsServiceData, sparkApp.getErrorMessage());
                    } else {
                        return updateServiceResult(jacsServiceData, sparkApp.getAppId());
                    }
                })
                ;
    }

    private SparkAppArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SparkAppArgs());
    }

    private Long getSparkClusterJobId(SparkAppArgs args) {
        return Long.valueOf(args.sparkJobId);
    }
}
