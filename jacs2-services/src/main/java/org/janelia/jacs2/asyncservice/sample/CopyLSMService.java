package org.janelia.jacs2.asyncservice.sample;

import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.domain.workflow.WorkflowImage;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;

/**
 * Create a temporary copy of an LSM file, unzipping it if necessary.
 */
@Named("copyLSM")
@ServiceParameter(name="filepath", type=String.class, description="Absolute filepath to the LSM")
public class CopyLSMService extends AbstractServiceProcessor<WorkflowImage> {

    @Inject
    CopyLSMService(ServiceComputationFactory computationFactory,
                   JacsServiceDataPersistence jacsServiceDataPersistence,
                   @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                   Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(CopyLSMService.class);
    }

    @Override
    public ServiceComputation<JacsServiceResult<WorkflowImage>> process(JacsServiceData jacsServiceData) {
        WorkflowImage image = new WorkflowImage();
        return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData, image));
    }

}
