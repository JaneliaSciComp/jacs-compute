package org.janelia.jacs2.asyncservice.spark;

import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Named("sparkProcessor")
public class SparkAppProcessor extends AbstractServiceProcessor<Void> {

    static class SparkAppArgs extends ServiceArgs {
        @Parameter(names = "-appLocation", description = "Spark application location", required = true)
        String appLocation;
        @Parameter(names = "-appEntryPoint", description = "Spark application entry point, i.e., java main class name")
        String appEntryPoint;
        @Parameter(names = "-appArgs", description = "Spark application arguments")
        List<String> appArgs = new ArrayList<>();

        SparkAppArgs() {
            super("Spark application processor");
        }
    }

    private final LSFSparkClusterLauncher clusterLauncher;
    private final int defaultNumNodes;

    @Inject
    SparkAppProcessor(ServiceComputationFactory computationFactory,
                      JacsServiceDataPersistence jacsServiceDataPersistence,
                      @StrPropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                      LSFSparkClusterLauncher clusterLauncher,
                      @IntPropertyValue(name = "service.spark.defaultNumNodes", defaultValue = 2) Integer defaultNumNodes,
                      Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.clusterLauncher = clusterLauncher;
        this.defaultNumNodes = defaultNumNodes <= 0 ? 1 : defaultNumNodes;
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

        // prepare service directories
        JacsServiceFolder serviceWorkingFolder = getWorkingDirectory(jacsServiceData);
        updateOutputAndErrorPaths(jacsServiceData);
        prepareDir(serviceWorkingFolder.getServiceFolder().toString());
        prepareDir(jacsServiceData.getOutputPath());
        prepareDir(jacsServiceData.getErrorPath());

        DataHolder<SparkCluster> runningClusterState = new DataHolder<>();
        return startCluster(jacsServiceData, serviceWorkingFolder)
                .thenCompose(sparkCluster -> {
                    runningClusterState.setData(sparkCluster);
                    jacsServiceDataPersistence.addServiceEvent(
                            jacsServiceData,
                            JacsServiceData.createServiceEvent(JacsServiceEventTypes.CLUSTER_SUBMIT, String.format("Running app using spark job %s", sparkCluster.getJobId())));
                    return sparkCluster.runApp(
                            args.appLocation,
                            args.appEntryPoint,
                            getDefaultParallelism(jacsServiceData.getResources()),
                            jacsServiceData.getOutputPath(),
                            jacsServiceData.getErrorPath(),
                            args.appArgs);
                })
                .whenComplete(((sparkApp, exc) -> {
                    if (runningClusterState.isPresent()) runningClusterState.getData().stopCluster();
                }))
                .thenApply(sparkApp -> new JacsServiceResult<>(jacsServiceData))
                ;
    }

    private ServiceComputation<SparkCluster> startCluster(JacsServiceData jacsServiceData, JacsServiceFolder serviceWorkingFolder) {
        return clusterLauncher.startCluster(jacsServiceData, getRequestedNodes(jacsServiceData.getResources()), serviceWorkingFolder.getServiceFolder(),
                getSparkDriverMemory(jacsServiceData.getResources()),
                getSparkExecutorMemory(jacsServiceData.getResources()),
                getSparkExecutorCores(jacsServiceData.getResources()),
                getSparkLogConfigFile(jacsServiceData.getResources()));
    }

    private int getRequestedNodes(Map<String, String> serviceResources) {
        String requestedNodes = StringUtils.defaultIfBlank(serviceResources.get("spark.numNodes"), "1");
        int numNodes = Integer.parseInt(requestedNodes);
        return numNodes <= 0 ? defaultNumNodes : numNodes;
    }

    private int getDefaultParallelism(Map<String, String> serviceResources) {
        String defaultParallelism = StringUtils.defaultIfBlank(serviceResources.get("spark.defaultParallelism"), "0");
        int parallelism = Integer.parseInt(defaultParallelism);
        return parallelism <= 0 ? 0 : parallelism;
    }

    private String getSparkDriverMemory(Map<String, String> serviceResources) {
        return serviceResources.get("spark.driverMemory");
    }

    private String getSparkExecutorMemory(Map<String, String> serviceResources) {
        return serviceResources.get("spark.executorMemory");
    }

    private int getSparkExecutorCores(Map<String, String> serviceResources) {
        String sparkExecutorCores = StringUtils.defaultIfBlank(serviceResources.get("spark.executorCores"), "0");
        int executorCores = Integer.parseInt(sparkExecutorCores);
        return executorCores <= 0 ? 0 : executorCores;
    }

    private String getSparkLogConfigFile(Map<String, String> serviceResources) {
        return serviceResources.get("spark.logConfigFile");
    }
}
