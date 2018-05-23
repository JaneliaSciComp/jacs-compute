package org.janelia.jacs2.asyncservice.workflow;

import org.janelia.jacs2.asyncservice.common.ServiceInterceptor;
import org.janelia.model.service.JacsServiceData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;

/**
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("workflowInterceptor")
public class WorkflowInterceptor implements ServiceInterceptor {

    private static final Logger log = LoggerFactory.getLogger(WorkflowInterceptor.class);

    @Override
    public void onDispatch(JacsServiceData jacsServiceData) {
        log.info("onDispatch {}", jacsServiceData.getId());
    }

    @Override
    public void beforeProcess(JacsServiceData jacsServiceData) {
        log.info("beforeProcess {}", jacsServiceData.getId());
    }

    @Override
    public void afterProcess(JacsServiceData jacsServiceData) {
        log.info("afterProcess {}", jacsServiceData.getId());
    }

    @Override
    public void andFinally(JacsServiceData jacsServiceData) {
        log.info("andFinally {}", jacsServiceData.getId());
    }
}
