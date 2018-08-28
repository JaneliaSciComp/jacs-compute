package org.janelia.jacs2.asyncservice.workflow;

import com.beust.jcommander.Parameter;
import org.janelia.dagobah.DAG;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.dataservice.nodes.FileStore;
import org.janelia.jacs2.dataservice.nodes.FileStoreNode;
import org.janelia.model.access.domain.DomainDAO;
import org.janelia.model.domain.sample.DataSet;
import org.janelia.model.domain.sample.LSMImage;
import org.janelia.model.domain.sample.Sample;
import org.janelia.model.domain.sample.SampleLock;
import org.janelia.model.domain.workflow.SamplePipelineConfiguration;
import org.janelia.model.domain.workflow.SamplePipelineOutput;
import org.janelia.model.domain.workflow.WorkflowTask;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Construct a workflow for a given Sample, and execute the workflow as a set of dependent JACS services.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("sampleWorkflow")
public class SampleWorkflowExecutor extends WorkflowExecutor<Sample> {

    static class SampleWorkflowExecutorArgs extends ServiceArgs {
        @Parameter(names = "-sampleId", description = "GUID of the sample to run", required = true)
        Long sampleId;
        @Parameter(names = "-force", description = "Which pipeline steps should be forced to reprocess", required = true, variableArity = true)
        List<String> force = new ArrayList<>();
    }

    @Inject
    private FileStore filestore;
    @Inject
    private DomainDAO domainDao;

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SampleWorkflowExecutor.class, new SampleWorkflowExecutorArgs());
    }

    @Override
    protected JacsServiceData prepareProcessing(JacsServiceData sd) throws Exception {

        // Create a file node for this pipeline execution and set it as a workspace so that all descendant services use it
        FileStoreNode sampleNode = filestore.createNode(sd.getOwnerName(), "Sample", sd.getId().longValue());
        jacsServiceDataPersistence.updateField(sd,"workspace", sampleNode.toPath().toString());

        SampleWorkflowExecutorArgs args = getArgs(sd);
        Long sampleId = args.sampleId;
        String owner = sd.getOwnerKey();
        Long taskId = sd.getId().longValue();

        // Lock the sample for the duration of the workflow
        SampleLock lock = domainDao.lockSample(owner, sampleId, taskId, "Sample Workflow");
        if (lock==null) {
            throw new IllegalStateException("Could not obtain lock on Sample#"+sampleId);
        }

        logger.info("Confirmed lock on Sample#"+sampleId);

        // TODO: create pipeline run


        return super.prepareProcessing(sd);
    }

    @Override
    protected DAG<WorkflowTask> createDAG(JacsServiceData sd) {

        SampleWorkflowExecutorArgs args = getArgs(sd);

        Long sampleId = args.sampleId;
        Set<SamplePipelineOutput> force = args.force.stream().map((SamplePipelineOutput::valueOf)).collect(Collectors.toSet());

        Sample sample = domainDao.getDomainObject(sd.getOwnerKey(), Sample.class, sampleId);
        if (sample==null) {
            throw new ComputationException(sd, "Could not find Sample#"+sampleId);
        }

        List<LSMImage> lsms = domainDao.getActiveLsmsBySampleId(sd.getOwnerKey(), sampleId);
        if (lsms==null || lsms.size() != sample.getLsmReferences().size()) {
            throw new ComputationException(sd, "Could not retrieve LSMs for Sample#"+sampleId);
        }

        DataSet dataSet = domainDao.getDataSetByIdentifier(sd.getOwnerKey(), sample.getDataSet());
        if (dataSet==null) {
            throw new ComputationException(sd, "Could not find data set "+sample.getDataSet());
        }

        // TODO: get this from data set
        SamplePipelineConfiguration config = new SamplePipelineConfiguration();

        SampleWorkflowGenerator workflowGen = new SampleWorkflowGenerator(config, force);
        DAG<WorkflowTask> dag = workflowGen.createPipeline(sample, lsms);

        return dag;
    }

    @Override
    protected void doFinally(JacsServiceData sd, Throwable throwable) {

        super.doFinally(sd, throwable);

        SampleWorkflowExecutorArgs args = getArgs(sd);
        Long sampleId = args.sampleId;
        String owner = sd.getOwnerKey();
        Long taskId = sd.getId().longValue();

        logger.info("Sample pipeline completed for Sample#"+sampleId);

        // Process errors
        if (throwable != null) {
            // TODO: mark Sample with status=Error

        }

        // TODO: delete files marked deleteOnExit

        // Unlock sample
        if (domainDao.unlockSample(owner, sampleId, taskId)) {
            logger.info("Unlocked Sample#"+sampleId);
        }
        else {
            logger.warn("Could not unlock Sample#"+sampleId);
        }
    }

    @Override
    protected Sample getResult(JacsServiceData jacsServiceData) {
        SampleWorkflowExecutorArgs args = getArgs(jacsServiceData);
        Long sampleId = args.sampleId;
        Sample sample = domainDao.getDomainObject(jacsServiceData.getOwnerKey(), Sample.class, sampleId);
        if (sample==null) {
            throw new ComputationException(jacsServiceData, "Could not find Sample#"+sampleId);
        }
        return sample;
    }

    private SampleWorkflowExecutorArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SampleWorkflowExecutorArgs());
    }
}
