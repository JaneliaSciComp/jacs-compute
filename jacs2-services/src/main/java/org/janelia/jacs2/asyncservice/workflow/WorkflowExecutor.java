package org.janelia.jacs2.asyncservice.workflow;

import com.google.common.util.concurrent.UncheckedExecutionException;
import org.janelia.dagobah.DAG;
import org.janelia.dagobah.WorkflowProcessorKt;
import org.janelia.jacs2.asyncservice.ServiceRegistry;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.model.access.domain.WorkflowDAO;
import org.janelia.model.domain.workflow.Workflow;
import org.janelia.model.domain.workflow.WorkflowTask;
import org.janelia.model.service.JacsServiceData;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Construct a workflow and execute it as a set of services.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public abstract class WorkflowExecutor<T> extends AbstractBasicLifeCycleServiceProcessor2<T> {

    @Inject
    private ServiceRegistry serviceRegistry;
    @Inject
    private WorkflowDAO workflowDao;

    @Override
    protected ServiceComputation<JacsServiceData> processing(JacsServiceData sd) {
        return computationFactory.newCompletedComputation(sd)
                .thenApply(this::submitServiceDependencies)
                .thenSuspendUntil(new WaitingForDependenciesContinuationCond<>(
                        (JacsServiceData pd) -> pd,
                        (JacsServiceData sd1, JacsServiceData sd2) -> sd1,
                        jacsServiceDataPersistence,
                        logger).negate());
    }


    protected JacsServiceData submitServiceDependencies(JacsServiceData sd) {
        try {
            DAG<WorkflowTask> dag = createDAG(sd);
            submitDAG(sd, dag);
            return sd;
        }
        catch (Exception e) {
            throw new UncheckedExecutionException(e);
        }
    }

    protected abstract DAG<WorkflowTask> createDAG(JacsServiceData sd);

    private void submitDAG(JacsServiceData sd, DAG<WorkflowTask> dag) {

        Workflow workflow = workflowDao.saveDAG(dag);
        logger.info("Saved new workflow as {}", workflow);

        Map<String, Object> globals = getGlobals(sd);
        if (!globals.isEmpty()) {
            workflow.setGlobals(getGlobals(sd));
            workflowDao.saveWorkflow(workflow);
            logger.info("Added globals to workflow: {}", globals);
        }

        jacsServiceDataPersistence.updateField(sd,"workflowId", workflow.getId());

        Map<Long, JacsServiceData> services = new HashMap<>();

        logger.info("Workflow executor is JacsServiceData#{}", sd.getId());

        for (WorkflowTask task : WorkflowProcessorKt.getTasksToRun(dag)) {
            logger.info("Processing {} (Task#{})", task.getName(), task.getId());

            // Instantiate service and cache it
            ServiceProcessor service = serviceRegistry.lookupService(task.getServiceClass());

            if (service==null) {
                throw new ComputationException(sd, "Could not find service class: "+task.getServiceClass());
            }

            // Find upstream dependencies
            List<JacsServiceData> upstream = new ArrayList<>();
            for (WorkflowTask upstreamTask : dag.getUpstream(task)) {

                JacsServiceData upstreamService = services.get(upstreamTask.getId());
                if (upstreamService==null) {
                    logger.info("  Upstream task does not need to run: Task#{}", upstreamTask.getId());
                }
                else {
                    logger.info("  Adding upstream Service#{}", upstreamService.getId());
                    upstream.add(upstreamService);
                }
            }

            // Submit a service to execute the task
            JacsServiceData dep = submitDependencyIfNotFound(
                    service.createServiceData(new ServiceExecutionContext.Builder(sd)
                            .addInterceptor(WorkflowInterceptor.class)
                            .description(task.getName())
                            .waitFor(upstream)
                            .workflowId(workflow.getId())
                            .taskId(task.getId())
                            .build()));

            services.put(task.getId(), dep);
            logger.info("  Submitted as Service#{}", dep.getId());
        }
    }

    protected JacsServiceData submitDependencyIfNotFound(JacsServiceData dependency) {
        return jacsServiceDataPersistence.createServiceIfNotFound(dependency);
    }

    protected Map<String, Object> getGlobals(JacsServiceData jacsServiceData) {
        return new HashMap<>();
    }

}
