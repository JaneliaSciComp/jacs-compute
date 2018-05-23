package org.janelia.jacs2.asyncservice.sample;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.fileservices.FileCopyProcessor;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.nodes.FileStore;
import org.janelia.jacs2.dataservice.nodes.FileStoreNode;
import org.janelia.jacs2.dataservice.nodes.FileStorePath;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.utils.ArchiveUtils;
import org.janelia.model.access.domain.DomainUtils;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.workflow.WorkflowImage;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;

/**
 * Create a temporary copy of an LSM file, unzipping it if necessary.
 */
@Named("copyLSM")
@ServiceParameter(name="lsm", type=WorkflowImage.class, description="Primary LSM image")
@ServiceResult(name="lsm", type=WorkflowImage.class, description="Temporary LSM image")
public class CopyLSMService extends AbstractServiceProcessor<WorkflowImage> {

    @Inject
    private FileStore filestore;
    private final WrappedServiceProcessor<FileCopyProcessor, File> fileCopyProcessor;

    @Inject
    CopyLSMService(ServiceComputationFactory computationFactory,
                   JacsServiceDataPersistence serviceDataPersistence,
                   @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                   FileCopyProcessor fileCopyProcessor,
                   Logger logger) {
        super(computationFactory, serviceDataPersistence, defaultWorkingDir, logger);
        this.fileCopyProcessor = new WrappedServiceProcessor<>(computationFactory, serviceDataPersistence, fileCopyProcessor);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(CopyLSMService.class);
    }

    @Override
    public ServiceComputation<JacsServiceResult<WorkflowImage>> process(JacsServiceData jacsServiceData) {

        WorkflowImage lsm = (WorkflowImage)jacsServiceData.getDictionaryArgs().get("lsm");
        String sourceFilepath = DomainUtils.getFilepath(lsm, FileType.LosslessStack);

        Path serviceFolder = getServiceFolder(jacsServiceData);

        String targetFilepath = getTargetLocation(sourceFilepath, serviceFolder);
        logger.info("targetFilepath: "+targetFilepath);

        return fileCopyProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Convert input file")
                        .build(),
                new ServiceArg("-src", sourceFilepath),
                new ServiceArg("-dst", targetFilepath))
                .thenApply((JacsServiceResult<File> result) -> {
                    // Create a new image based on the primary image
                    WorkflowImage outputImage = new WorkflowImage(lsm);
                    // Clear existing files
                    outputImage.setFiles(new HashMap<>());
                    // Add the temporary stack
                    DomainUtils.setFilepath(outputImage, FileType.LosslessStack, targetFilepath);
                    // Ensure it gets deleted once the workflow is done
                    outputImage.setDeleteOnExit(Sets.newHashSet(FileType.LosslessStack));
                    // Return the single result
                    return new JacsServiceResult(jacsServiceData, outputImage);
                });
    }

    private String getTargetLocation(String sourceFile, Path targetFileNode) {
        if (sourceFile==null) return null;
        File file = new File(sourceFile);
        String filename = ArchiveUtils.getDecompressedFilepath(file.getName());
        return targetFileNode.resolve(filename).toString();
    }
}
