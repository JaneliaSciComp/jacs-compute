package org.janelia.jacs2.asyncservice.spark;

import javax.inject.Inject;
import javax.inject.Named;

import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.cdi.qualifier.StrPropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

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
        super(computationFactory,
                jacsServiceDataPersistence,
                defaultWorkingDir,
                clusterLauncher,
                null,
                null,
                1,
                0,
                -1,
                null,
                null,
                logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SparkClusterStopProcessor.class, new StopSparkJobArgs());
    }

    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        StopSparkJobArgs args = getArgs(jacsServiceData);

        return computationFactory.newCompletedComputation(sparkClusterLauncher.createSparkCluster(
                args.getSparkMasterJobId(),
                args.getSparkWorkerJobId(),
                null /* spark URI is not important here because we only want to destroy it anyway */))
                .thenApply(sparkCluster -> {
                    jacsServiceDataPersistence.addServiceEvent(
                            jacsServiceData,
                            JacsServiceData.createServiceEvent(JacsServiceEventTypes.CLUSTER_STOP_JOB,
                                    String.format("Stop spark cluster on %s (%s, %s)",
                                            sparkCluster.getSparkClusterInfo().getMasterURI(),
                                            sparkCluster.getSparkClusterInfo().getMasterJobId(),
                                            sparkCluster.getSparkClusterInfo().getWorkerJobId())));

                    sparkCluster.stopCluster();
                    return new JacsServiceResult<>(jacsServiceData);
                })
                ;
    }

    private StopSparkJobArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new StopSparkJobArgs());
    }
}
