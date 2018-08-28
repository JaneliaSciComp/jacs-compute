package org.janelia.jacs2.asyncservice.sample;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Sets;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.fileservices.FileCopyProcessor;
import org.janelia.jacs2.utils.ArchiveUtils;
import org.janelia.model.access.domain.DomainUtils;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.sample.LSMImage;
import org.janelia.model.domain.workflow.WorkflowImage;
import org.janelia.model.service.JacsServiceData;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

@Named("copyLSM")

@Service(description="Create a temporary copy of an LSM file in the filestore, unzipping it if necessary")

@ServiceInput(name="lsm",
        type=LSMImage.class,
        description="Primary LSM image")

@ServiceResult(
        name="outputImage",
        type=WorkflowImage.class,
        description="Temporary LSM image")

public class CopyLSMService extends AbstractServiceProcessor2<WorkflowImage> {

    @Inject
    private FileCopyProcessor fileCopyProcessor;

    @Override
    public ServiceComputation<JacsServiceResult<WorkflowImage>> process(JacsServiceData sd) {

        Path serviceFolder = getServiceFolder(sd);
        Map<String, Object> args = sd.getDictionaryArgs();
        LSMImage lsm = (LSMImage)args.get("lsm");

        String sourceFilepath = DomainUtils.getFilepath(lsm, FileType.LosslessStack);
        if (sourceFilepath == null) {
            throw new IllegalArgumentException("Source LSM has no lossless stack");
        }

        Path targetFilepath = getTargetLocation(sourceFilepath, serviceFolder);

        return inline(fileCopyProcessor).process(getContext(sd,"Copy primary image"),
                new ServiceArg("-src", sourceFilepath),
                new ServiceArg("-dst", targetFilepath.toString()))
                .thenApply((JacsServiceResult<File> result) -> {
                    // Create a new image based on the primary image
                    WorkflowImage outputImage = getWorkflowImage(lsm);
                    // Clear existing files
                    outputImage.setFiles(new HashMap<>());
                    // Add the temporary stack
                    DomainUtils.setFilepath(outputImage, FileType.LosslessStack, targetFilepath.toString());
                    // Ensure it gets deleted once the workflow is done
                    outputImage.setDeleteOnExit(Sets.newHashSet(FileType.LosslessStack));
                    // Return the single result
                    return updateServiceResult(sd, outputImage);
                });
    }

    private WorkflowImage getWorkflowImage(LSMImage lsm) {

        WorkflowImage image = new WorkflowImage();
        image.setOwnerKey(lsm.getOwnerKey());
        image.setName(lsm.getName());
        image.setAnatomicalArea(lsm.getAnatomicalArea());
        image.setTile(lsm.getTile());
        image.setChannelColors(lsm.getChannelColors());
        image.setChannelSpec(lsm.getChanSpec());
        image.setImageSize(lsm.getImageSize());
        image.setOpticalResolution(lsm.getOpticalResolution());

        return image;
    }

    private Path getTargetLocation(String sourceFile, Path targetFileNode) {
        if (sourceFile==null) return null;
        File file = new File(sourceFile);
        String filename = ArchiveUtils.getDecompressedFilepath(file.getName());
        return targetFileNode.resolve("temp").resolve(filename);
    }
}
