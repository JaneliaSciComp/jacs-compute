package org.janelia.jacs2.asyncservice.sample;

import com.google.common.collect.Sets;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.fileservices.FileCopyProcessor;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.domain.DomainUtils;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.workflow.WorkflowImage;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;

/**
 * Create a temporary copy of an LSM file, unzipping it if necessary.
 */
@Named("copyLSM")
@ServiceParameter(name="lsm", type=WorkflowImage.class, description="LSM image")
public class CopyLSMService extends AbstractServiceProcessor<WorkflowImage> {

    private final WrappedServiceProcessor<FileCopyProcessor, File> fileCopyProcessor;

    @Inject
    CopyLSMService(ServiceComputationFactory computationFactory,
                   JacsServiceDataPersistence jacsServiceDataPersistence,
                   @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                   FileCopyProcessor fileCopyProcessor,
                   Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.fileCopyProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, fileCopyProcessor);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(CopyLSMService.class);
    }

    @Override
    public ServiceComputation<JacsServiceResult<WorkflowImage>> process(JacsServiceData jacsServiceData) {

        WorkflowImage lsm = (WorkflowImage)jacsServiceData.getDictionaryArgs().get("lsm");
        String sourceFilepath = DomainUtils.getFilepath(lsm, FileType.LosslessStack);
        String targetFilepath = "";

        return fileCopyProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Convert input file")
                        .build(),
                    new ServiceArg("-src", sourceFilepath),
                    new ServiceArg("-dst", targetFilepath))
                .thenApply((JacsServiceResult<File> result) -> {
                    WorkflowImage outputImage = new WorkflowImage(lsm);
                    DomainUtils.setFilepath(outputImage, FileType.LosslessStack, targetFilepath);
                    outputImage.setDeleteOnExit(Sets.newHashSet(FileType.LosslessStack));
                    return new JacsServiceResult(jacsServiceData, outputImage);
                });
    }

}
