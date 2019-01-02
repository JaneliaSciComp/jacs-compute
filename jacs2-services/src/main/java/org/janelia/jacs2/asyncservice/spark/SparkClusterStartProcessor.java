package org.janelia.jacs2.asyncservice.spark;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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

import javax.inject.Inject;
import javax.inject.Named;

@Named("startSparkCluster")
public class SparkClusterStartProcessor extends AbstractSparkProcessor<SparkClusterStartProcessor.SparkJobInfo> {

    static class StartSparkJobArgs extends SparkArgs {
        StartSparkJobArgs() {
            super("Start spark cluster");
        }
    }

    static class SparkJobInfo {
        private final Long jobId;
        private final String masterURI;

        @JsonCreator
        SparkJobInfo(@JsonProperty("jobId") Long jobId,
                     @JsonProperty("masterURI") String masterURI) {
            this.jobId = jobId;
            this.masterURI = masterURI;
        }

        public Long getJobId() {
            return jobId;
        }

        public String getMasterURI() {
            return masterURI;
        }
    }

    private final ComputeAccounting accounting;

    @Inject
    SparkClusterStartProcessor(ServiceComputationFactory computationFactory,
                               JacsServiceDataPersistence jacsServiceDataPersistence,
                               @StrPropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                               BatchLSFSparkClusterLauncher clusterLauncher,
                               ComputeAccounting accounting,
                               @IntPropertyValue(name = "service.spark.defaultNumNodes", defaultValue = 2) Integer defaultNumNodes,
                               Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, clusterLauncher, defaultNumNodes, logger);
        this.accounting = accounting;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SparkClusterStartProcessor.class, new StartSparkJobArgs());
    }

    @Override
    public ServiceResultHandler<SparkJobInfo> getResultHandler() {
        return new AbstractAnyServiceResultHandler<SparkJobInfo>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @SuppressWarnings("unchecked")
            @Override
            public SparkJobInfo collectResult(JacsServiceResult<?> depResults) {
                JacsServiceResult<SparkJobInfo> intermediateResult = (JacsServiceResult<SparkJobInfo>)depResults;
                return intermediateResult.getResult();
            }

            @Override
            public SparkJobInfo getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<SparkJobInfo>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<SparkJobInfo>> process(JacsServiceData jacsServiceData) {
        // prepare service directories
        JacsServiceFolder serviceWorkingFolder = prepareSparkJobDirs(jacsServiceData);

        return startCluster(jacsServiceData, serviceWorkingFolder)
                .thenApply(sparkCluster -> {
                            jacsServiceDataPersistence.addServiceEvent(
                                    jacsServiceData,
                                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.CLUSTER_SUBMIT,
                                            String.format("Started spark cluster %s (%s)",
                                                    sparkCluster.getMasterURI(),
                                                    sparkCluster.getMasterJobId())));
                            return updateServiceResult(
                                    jacsServiceData,
                                    new SparkJobInfo(sparkCluster.getMasterJobId(), sparkCluster.getMasterURI())
                            );
                })
                ;
    }

    private ServiceComputation<SparkCluster> startCluster(JacsServiceData jacsServiceData, JacsServiceFolder serviceWorkingFolder) {
        return sparkClusterLauncher.startCluster(
                getRequestedNodes(jacsServiceData.getResources()),
                serviceWorkingFolder.getServiceFolder(),
                accounting.getComputeAccount(jacsServiceData),
                getSparkDriverMemory(jacsServiceData.getResources()),
                getSparkExecutorMemory(jacsServiceData.getResources()),
                getSparkLogConfigFile(jacsServiceData.getResources()));
    }
}
