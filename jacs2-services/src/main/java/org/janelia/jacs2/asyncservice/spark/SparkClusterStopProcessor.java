package org.janelia.jacs2.asyncservice.spark;

import com.beust.jcommander.Parameter;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.StrPropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;

@Named("stopSparkCluster")
public class SparkClusterStopProcessor extends AbstractSparkProcessor<Void> {

    static class StopSparkJobArgs extends SparkClusterArgs {
        StopSparkJobArgs() {
            super("Stop spark cluster");
        }
    }

    @Inject
    SparkClusterStopProcessor(ServiceComputationFactory computationFactory,
                              JacsServiceDataPersistence jacsServiceDataPersistence,
                              @StrPropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                              LSFSparkClusterLauncher clusterLauncher,
                              Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, clusterLauncher, 1, 0, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SparkClusterStopProcessor.class, new StopSparkJobArgs());
    }

    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        StopSparkJobArgs args = getArgs(jacsServiceData);

        return sparkClusterLauncher.createCluster(args.getSparkClusterJobId(),
                args.getSparkWorkerJobIds(),
                args.minSparkWorkers,
                sparkClusterLauncher.calculateDefaultParallelism(getRequestedNodes(jacsServiceData.getResources())),
                getSparkDriverMemory(jacsServiceData.getResources()),
                getSparkExecutorMemory(jacsServiceData.getResources()),
                getSparkLogConfigFile(jacsServiceData.getResources()))
                .thenApply(sparkCluster -> {
                    jacsServiceDataPersistence.addServiceEvent(
                            jacsServiceData,
                            JacsServiceData.createServiceEvent(JacsServiceEventTypes.CLUSTER_STOP_JOB,
                                    String.format("Stop spark cluster on %s (%s)",
                                            sparkCluster.getMasterURI(),
                                            sparkCluster.getMasterJobId())));

                    sparkCluster.stopCluster();
                    return new JacsServiceResult<>(jacsServiceData);
                })
                ;
    }

    private StopSparkJobArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new StopSparkJobArgs());
    }
}
