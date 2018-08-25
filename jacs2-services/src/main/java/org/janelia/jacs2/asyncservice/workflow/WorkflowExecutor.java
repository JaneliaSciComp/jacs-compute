package org.janelia.jacs2.asyncservice.workflow;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.NotImplementedException;
import org.janelia.dagobah.DAG;
import org.janelia.dagobah.WorkflowProcessorKt;
import org.janelia.jacs2.asyncservice.ServiceRegistry;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
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
    public ServiceResultHandler<T> getResultHandler() {
        return new AbstractAnyServiceResultHandler<T>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public T collectResult(JacsServiceResult<?> depResults) {
                JacsServiceData jacsServiceData = depResults.getJacsServiceData();
                return getResult(jacsServiceData);
            }

            @Override
            public T getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(
                        jacsServiceData.getSerializableResult(), new TypeReference<T>() {});
            }
        };
    }

    @Override
    protected JacsServiceResult<Void> submitServiceDependencies(JacsServiceData sd) {
        DAG<WorkflowTask> dag = createDAG(sd);
        submitDAG(sd, dag);
        return new JacsServiceResult<>(sd);
    }

    protected abstract DAG<WorkflowTask> createDAG(JacsServiceData sd);

    protected abstract T getResult(JacsServiceData jacsServiceData);

    private void submitDAG(JacsServiceData sd, DAG<WorkflowTask> dag) {

        Workflow workflow = workflowDao.saveDAG(dag);
        logger.info("Saved new workflow as {}", workflow);

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

    @Override
    protected void doFinally(JacsServiceData sd, Throwable throwable) {
    }

    @Override
    protected ServiceComputation<JacsServiceResult<Void>> processing(JacsServiceResult<Void> depResults) {
        return computationFactory.newCompletedComputation(depResults);
    }
}
