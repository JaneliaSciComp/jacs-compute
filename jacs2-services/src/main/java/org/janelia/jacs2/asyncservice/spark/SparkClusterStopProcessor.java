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
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;

@Named("stopSparkCluster")
public class SparkClusterStopProcessor extends AbstractSparkProcessor<Void> {

    static class StopSparkJobArgs extends SparkArgs {
        @Parameter(names = "-sparkJobId", description = "Spark cluster ID")
        Long sparkJobId;

        StopSparkJobArgs() {
            super("Stop spark cluster");
        }
    }


    @Inject
    SparkClusterStopProcessor(ServiceComputationFactory computationFactory,
                              JacsServiceDataPersistence jacsServiceDataPersistence,
                              @StrPropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                              LSFSparkClusterLauncher clusterLauncher,
                              @IntPropertyValue(name = "service.spark.defaultNumNodes", defaultValue = 2) Integer defaultNumNodes,
                              Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, clusterLauncher, defaultNumNodes, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SparkClusterStopProcessor.class, new StopSparkJobArgs());
    }

    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        StopSparkJobArgs args = getArgs(jacsServiceData);

        return clusterLauncher.createCluster(args.sparkJobId,
                clusterLauncher.calculateDefaultParallelism(getRequestedNodes(jacsServiceData.getResources())),
                getSparkDriverMemory(jacsServiceData.getResources()),
                getSparkExecutorMemory(jacsServiceData.getResources()),
                getSparkExecutorCores(jacsServiceData.getResources()),
                getSparkLogConfigFile(jacsServiceData.getResources()))
                .thenApply(sparkCluster -> {
                    sparkCluster.stopCluster();
                    return new JacsServiceResult<>(jacsServiceData);
                })
                ;
    }

    private StopSparkJobArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new StopSparkJobArgs());
    }

}
