package org.janelia.jacs2.asyncservice.workflow;

import com.beust.jcommander.Parameter;
import org.janelia.dagobah.DAG;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.sample.helpers.SampleHelper;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.dataservice.nodes.FileStore;
import org.janelia.jacs2.dataservice.nodes.FileStoreNode;
import org.janelia.model.access.domain.DomainDAO;
import org.janelia.model.domain.sample.*;
import org.janelia.model.domain.workflow.SamplePipelineConfiguration;
import org.janelia.model.domain.workflow.SamplePipelineOutput;
import org.janelia.model.domain.workflow.WorkflowTask;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
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

    @Inject
    private SampleHelper sampleHelper;

    private List<SamplePipelineRun> pipelineRuns = new ArrayList<>();

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SampleWorkflowExecutor.class, new SampleWorkflowExecutorArgs());
    }

    @Override
    protected JacsServiceData prepareProcessing(JacsServiceData sd) throws Exception {

        // Get arguments
        SampleWorkflowExecutorArgs args = getArgs(sd);
        Long sampleId = args.sampleId;

        // First, ensure that we are running as the sample owner.
        // TODO: in the future, do an auth check here
        Sample sample = domainDao.getDomainObject(sd.getOwnerKey(), Sample.class, sampleId);
        if (sample==null) {
            throw new ComputationException(sd, "Could not find Sample#"+sampleId);
        }
        jacsServiceDataPersistence.updateField(sd,"ownerKey", sample.getOwnerKey());

        // Create a file node for this pipeline execution and set it as a workspace so that all descendant services use it
        FileStoreNode sampleNode = filestore.createNode(sd.getOwnerName(), "Sample", sd.getId().longValue());
        jacsServiceDataPersistence.updateField(sd,"workspace", sampleNode.toPath().toString());

        // Update service data
        currentService.setJacsServiceData(sd);
        logger.info("UPDATED SD to "+sd);

        // Lock the sample for the duration of the workflow
        SampleLock lock = sampleHelper.lockSample(sampleId, "Sample Workflow");
        if (lock==null) {
            throw new IllegalStateException("Could not obtain lock on Sample#"+sampleId);
        }
        logger.info("Confirmed lock on Sample#"+sampleId);

        String pipelineName = "Sample Pipeline";
        String pipelineProcess = "samplePipeline";
        int pipelineVersion = 1;

        for (ObjectiveSample objectiveSample : sample.getObjectiveSamples()) {
            SamplePipelineRun pipelineRun = sampleHelper.addNewPipelineRun(pipelineName, pipelineProcess, pipelineVersion);
            objectiveSample.addRun(pipelineRun);
            pipelineRuns.add(pipelineRun);
        }

        sampleHelper.saveSample(sample);

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
        DAG<WorkflowTask> dag = workflowGen.createPipeline(sample, lsms, pipelineRuns);

        return dag;
    }

    @Override
    protected Map<String, Object> getGlobals(JacsServiceData sd) {

        SampleWorkflowExecutorArgs args = getArgs(sd);

        Map<String, Object>  globals = new HashMap<>();
        globals.put("sampleId", args.sampleId);
        globals.put("debug", false);
        return globals;
    }

    @Override
    protected void doFinally(JacsServiceData sd, Throwable throwable) {

        SampleWorkflowExecutorArgs args = getArgs(sd);
        Long sampleId = args.sampleId;
        String owner = sd.getOwnerKey();
        Long taskId = sd.getId().longValue();

        logger.info("Sample pipeline completed for Sample#" + sampleId);

        // Process errors
        if (throwable != null) {
            // TODO: mark Sample with status=Error

        }

        // TODO: delete files marked deleteOnExit

        // Delete any temp/tmp directories in the workspace
        if (sd.getWorkspace() != null) {
            logger.info("Looking for temp files to delete in {}", sd.getWorkspace());
            FileUtils.lookupFiles(Paths.get(sd.getWorkspace()), 10, "**/{temp,tmp.*}").forEach(tempPath -> {
                logger.info("Deleting {}", tempPath);
                try {
                    FileUtils.deleteDirectory(tempPath.toFile());
                } catch (IOException e) {
                    logger.error("Error deleting " + tempPath, e);
                }
            });
        }

        // Unlock sample
        if (sampleHelper.unlockSample(sampleId)) {
            logger.info("Unlocked Sample#" + sampleId);
        } else {
            logger.warn("Could not unlock Sample#" + sampleId);
        }
    }

    @Override
    protected Sample createResult() {
        JacsServiceData jacsServiceData = currentService.getJacsServiceData();
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
