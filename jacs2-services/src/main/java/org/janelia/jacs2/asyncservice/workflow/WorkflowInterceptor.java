package org.janelia.jacs2.asyncservice.workflow;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.janelia.dagobah.DAG;
import org.janelia.dagobah.TaskStatus;
import org.janelia.jacs2.asyncservice.ServiceRegistry;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ServiceInterceptor;
import org.janelia.jacs2.asyncservice.common.mdc.MdcContext;
import org.janelia.jacs2.asyncservice.sample.ServiceInput;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.domain.WorkflowDAO;
import org.janelia.model.domain.workflow.Workflow;
import org.janelia.model.domain.workflow.WorkflowTask;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.janelia.model.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.Collection;

/**
 * An interceptor which is tied to every service that executes via the WorkflowExecutor. These services are tied
 * to WorkflowTasks, and those tasks must be kept in sync with the service executions.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@MdcContext
@Named("workflowInterceptor")
public class WorkflowInterceptor implements ServiceInterceptor {

    private static final Logger log = LoggerFactory.getLogger(WorkflowInterceptor.class);

    @Inject
    private JacsServiceDataPersistence jacsServiceDataPersistence;

    @Inject
    private ServiceRegistry serviceRegistry;

    @Inject
    private WorkflowDAO workflowDao;

    /**
     * Called immediately before a task in a workflow will be executed. Propagates any parameters from upstream
     * tasks, and marks the task as running.
     * @param sd
     */
    @Override
    public void beforeProcess(JacsServiceData sd) {

        Workflow workflow = getWorkflow(sd);
        WorkflowTask task = getTask(sd);
        log.info("{} in {} is about to begin", task, workflow);

        // Prepare the task to run
        propagateObjects(workflow, task);
        task.setStatus(TaskStatus.Running);
        workflowDao.saveTask(task);

        // Also update JacsServiceData
        sd.getDictionaryArgs().putAll(task.getInputs());
        jacsServiceDataPersistence.save(sd);
    }

    /**
     * Prepare the given task for running by looking at its upstream tasks and copying their outputs
     * into the task's input slots. Currently, this is done by comparing class types of the outputs
     * and inputs, but future implementations may provide a named mapping capability.
     * @param workflow
     * @param task
     */
    private void propagateObjects(Workflow workflow, WorkflowTask task) {

        log.info("Propagating objects for {}", task);

        DAG<WorkflowTask> dag = workflowDao.getDAG(workflow);

        // First, take all outputs and group them by type
        Multimap<Class<?>,Object> outputByType = ArrayListMultimap.create();
        for (WorkflowTask upstreamTask : dag.getUpstream(task)) {

            log.info("  Propagating outputs of upstream {} (status={})", upstreamTask, upstreamTask.getStatus());

            if (!upstreamTask.getStatus().isFinal()) {
                throw new ComputationException("Upstream task is not complete: "+upstreamTask.getId());
            }
            if (upstreamTask.getStatus()!=TaskStatus.Complete) {
                throw new ComputationException("Upstream task was not successful: "+upstreamTask.getId());
            }

            for (Object obj : upstreamTask.getOutputs().values()) {
                if (obj!=null) {
                    log.info("  Found service output of type {}", obj.getClass().getSimpleName());
                    outputByType.put(obj.getClass(), obj);
                }
            }
        }

        // Now inject the types into their respective input slots
        ServiceMetaData serviceMetadata = serviceRegistry.getServiceMetadata(task.getServiceClass());

        for (ServiceInput serviceInput : serviceMetadata.getServiceInputs()) {

            Collection<Object> objects = outputByType.get(serviceInput.type());
            if (objects == null || objects.isEmpty()) {
                if (task.getInputs().get(serviceInput.name()) != null) {
                    log.trace("Service already has an input for {}",serviceInput.name());
                    continue;
                }
                else {
                    Object globalValue = workflow.getGlobals().get(serviceInput.name());
                    if (globalValue!=null) {
                        log.info("Using global value of {}", serviceInput.name());
                        addInput(task, serviceInput.name(), globalValue);
                        continue;
                    }
                    throw new ComputationException("No available inputs of type " +
                            serviceInput.type().getSimpleName());
                }
            }

            if (serviceInput.variadic()) {
                // Translate the Multimap internal list into an ArrayList to make it easier for Jackson
                addInput(task, serviceInput.name(), new ArrayList<>(objects));
            }
            else {
                if (objects.size()>1) {
                    throw new ComputationException("More than one output of type "+
                            serviceInput.type().getSimpleName()+
                            " for non-variadic service input "+serviceInput.name());
                }

                addInput(task, serviceInput.name(), objects.iterator().next());
            }

            log.info("Populated service input {}", serviceInput.name());
        }
    }

    private void addInput(WorkflowTask task, String name, Object value) {
        if (task.getInputs().get(name) != null) {
            log.warn("Overriding existing task input for {}", name);
        }
        task.getInputs().put(name, value);
    }

    /**
     * Runs after a task has finished, updates the task status, and copies the service output to the task.
     */
    @Override
    public void andFinally(JacsServiceData sd) {
        WorkflowTask task = getTask(sd);
        log.info("{} in Workflow#{} has completed", task, sd.getWorkflowId());
        if (sd.hasCompletedSuccessfully()) {
            // Mark task completed
            task.setStatus(TaskStatus.Complete);
            // Copy the service output to the task
            ServiceMetaData serviceMetadata = serviceRegistry.getServiceMetadata(task.getServiceClass());
            String resultName = serviceMetadata.getServiceResult().name();
            task.setOutputs(Utils.strObjMap(resultName, sd.getSerializableResult()));
        }
        else {
            // Mark task as having an error
            task.setStatus(TaskStatus.Error);
        }
        workflowDao.saveTask(task);
    }

    /**
     * Looks up the associated workflow for the given service.
     * @param sd
     * @return
     */
    private Workflow getWorkflow(JacsServiceData sd) {
        Long workflowId = sd.getWorkflowId();
        if (workflowId==null) {
            throw new ComputationException(sd,
                    "Workflow interceptor called on JacsServiceData#"+sd.getId()+" without workflow");
        }
        Workflow workflow = workflowDao.getWorkflow(workflowId);
        if (workflow==null) {
            throw new ComputationException(sd,
                    "Workflow interceptor called on JacsServiceData#"+sd.getId()+" with missing Workflow#"+workflowId);
        }
        return workflow;
    }

    /**
     * Looks up the workflow task for the given service.
     * @param sd
     * @return
     */
    private WorkflowTask getTask(JacsServiceData sd) {
        Long taskId = sd.getTaskId();
        if (taskId==null) {
            throw new ComputationException(sd,
                    "Workflow interceptor called on service without task");
        }
        WorkflowTask task = workflowDao.getWorkflowTask(taskId);
        if (task==null) {
            throw new ComputationException(sd,
                    "Workflow interceptor called on service with missing Task#"+taskId);
        }
        return task;
    }
}
