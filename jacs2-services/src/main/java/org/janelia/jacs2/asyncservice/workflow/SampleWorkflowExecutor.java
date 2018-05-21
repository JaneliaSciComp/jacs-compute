package org.janelia.jacs2.asyncservice.workflow;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.janelia.dagobah.DAG;
import org.janelia.dagobah.WorkflowProcessorKt;
import org.janelia.jacs2.asyncservice.ServiceRegistry;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.domain.sample.DataSet;
import org.janelia.model.domain.sample.LSMImage;
import org.janelia.model.domain.sample.Sample;
import org.janelia.model.domain.workflow.SamplePipelineConfiguration;
import org.janelia.model.domain.workflow.SamplePipelineOutput;
import org.janelia.model.domain.workflow.WorkflowTask;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Executes a workflow as a set of services.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("sampleWorkflow")
public class SampleWorkflowExecutor extends AbstractBasicLifeCycleServiceProcessor<Sample, Void> {

    static class SampleWorkflowExecutorArgs extends ServiceArgs {
        @Parameter(names = "-sampleId", description = "GUID of the sample to run", required = true)
        Long sampleId;
        @Parameter(names = "-force", description = "Which pipeline steps should be forced to reprocess", required = true, variableArity = true)
        List<String> force;
    }

    @Inject
    private ServiceRegistry serviceRegistry;
    @Inject
    private Instance<Object> creator;
    private final LegacyDomainDao legacyDomainDao;

    @Inject
    SampleWorkflowExecutor(ServiceComputationFactory computationFactory,
                           JacsServiceDataPersistence jacsServiceDataPersistence,
                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                           LegacyDomainDao legacyDomainDao,
                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.legacyDomainDao = legacyDomainDao;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SampleWorkflowExecutor.class, new SampleWorkflowExecutorArgs());
    }

    @Override
    public ServiceResultHandler<Sample> getResultHandler() {
        return new AbstractAnyServiceResultHandler<Sample>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public Sample collectResult(JacsServiceResult<?> depResults) {
                JacsServiceData jacsServiceData = depResults.getJacsServiceData();
                SampleWorkflowExecutorArgs args = getArgs(jacsServiceData);
                Long sampleId = args.sampleId;
                Sample sample = legacyDomainDao.getDomainObject(jacsServiceData.getOwnerKey(), Sample.class, sampleId);
                if (sample==null) {
                    throw new ComputationException(jacsServiceData, "Could not find Sample#"+sampleId);
                }
                return sample;
            }

            @Override
            public Sample getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<Sample>() {});
            }
        };
    }

    @Override
    protected JacsServiceData prepareProcessing(JacsServiceData jacsServiceData) {
        // TODO: lock sample
        return super.prepareProcessing(jacsServiceData);
    }

    @Override
    protected JacsServiceResult<Void> submitServiceDependencies(JacsServiceData jacsServiceData) {

        SampleWorkflowExecutorArgs args = getArgs(jacsServiceData);

        Long sampleId = args.sampleId;
        Set<SamplePipelineOutput> force = args.force.stream().map((SamplePipelineOutput::valueOf)).collect(Collectors.toSet());

        Sample sample = legacyDomainDao.getDomainObject(jacsServiceData.getOwnerKey(), Sample.class, sampleId);
        if (sample==null) {
            throw new ComputationException(jacsServiceData, "Could not find Sample#"+sampleId);
        }

        List<LSMImage> lsms = legacyDomainDao.getActiveLsmsBySampleId(jacsServiceData.getOwnerKey(), sampleId);
        if (lsms==null || lsms.size() != sample.getLsmReferences().size()) {
            throw new ComputationException(jacsServiceData, "Could not retrieve LSMs for Sample#"+sampleId);
        }

        DataSet dataSet = legacyDomainDao.getDataSetByIdentifier(jacsServiceData.getOwnerKey(), sample.getDataSet());
        if (dataSet==null) {
            throw new ComputationException(jacsServiceData, "Could not find data set "+sample.getDataSet());
        }

        // TODO: get this from data set
        SamplePipelineConfiguration config = new SamplePipelineConfiguration();

        SampleWorkflow workflow = new SampleWorkflow(config, force);
        DAG<WorkflowTask> dag = workflow.createPipeline(sample, lsms);

        Map<Long, JacsServiceData> services = new HashMap<>();

        for (WorkflowTask task : WorkflowProcessorKt.getTasksToRun(dag)) {
            logger.info("Processing Task#{}", task.getId());

            // Instantiate service and cache it
            ServiceProcessor service = serviceRegistry.lookupService(task.getServiceClass());

            // Find upstream dependencies
            List<JacsServiceData> upstream = new ArrayList<>();
            for (WorkflowTask upstreamTask : dag.getUpstream(task)) {

                JacsServiceData upstreamService = services.get(upstreamTask.getId());
                if (upstreamService==null) {
                    // TODO: task does not need to run, take its outputs and use them
                    logger.info("  Upstream task does not need to run: Task#{}", upstreamTask.getId());
                }
                else {
                    logger.info("  Adding upstream Service#{}", upstreamService.getId());
                    upstream.add(upstreamService);
                }
            }

            // Submit service to run
            JacsServiceData serviceData = submitDependencyIfNotFound(
                    service.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                           .description(task.getName())
                           .addDictionaryArgs(task.getInputs())
                           .waitFor(upstream)
                           .build()));

            services.put(task.getId(), serviceData);
            logger.info("Submitted {} as Service#{}", task.getName(), task.getId(), serviceData.getId());
        }

        return new JacsServiceResult<>(jacsServiceData);
    }

    @Override
    protected ServiceComputation<JacsServiceResult<Void>> processing(JacsServiceResult<Void> depResults) {
        return computationFactory.newCompletedComputation(depResults);
    }

    @Override
    protected JacsServiceResult<Sample> doFinally(JacsServiceResult<Sample> sr, Throwable throwable) {

        // TODO: Unlock sample
        // TODO: delete files marked deleteOnExit

        return sr;
    }

    private SampleWorkflowExecutorArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SampleWorkflowExecutorArgs());
    }
}
