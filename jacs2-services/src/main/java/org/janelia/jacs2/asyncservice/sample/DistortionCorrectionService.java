package org.janelia.jacs2.asyncservice.sample;

import com.google.common.collect.Sets;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor2;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.access.domain.DomainUtils;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.workflow.WorkflowImage;
import org.janelia.model.service.JacsServiceData;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Named("distortionCorrection")

@Service(description="Apply appropriate distortion correction to an LSM based on the microscope which was used to capture it")

@ServiceInput(
        name="inputImage",
        type=WorkflowImage.class,
        description="Uncorrected input image")

@ServiceOutput(
        name="outputImage",
        type=WorkflowImage.class,
        description="Corrected image stack")

public class DistortionCorrectionService extends AbstractServiceProcessor2<WorkflowImage> {

    private static final SimpleDateFormat dcDateTimeFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
    private static final String LSM_SUFFIX = ".lsm";
    private static final String V3DRAW_SUFFIX = ".v3draw";

    @Inject @PropertyValue(name = "DistortionCorrection.Macro.Path")
    private String macroFilepath;

    @Inject @PropertyValue(name = "DistortionCorrection.Fields.Path")
    private String fieldsFilepath;

    @Inject
    private FijiMacroService fijiMacroService;

    @Override
    public ServiceComputation<JacsServiceResult<WorkflowImage>> process(JacsServiceData sd) {

        Path serviceFolder = getServiceFolder(sd);

        Map<String, Object> args = sd.getDictionaryArgs();
        WorkflowImage inputImage = (WorkflowImage)args.get("inputImage");

        String filepath = DomainUtils.getFilepath(inputImage, FileType.LosslessStack);
        if (filepath==null) {
            throw new IllegalArgumentException("Input image has no lossless filepath");
        }

        String microscope = inputImage.getMicroscope();
        if (microscope==null) {
            throw new IllegalArgumentException("Input image has no microscope");
        }

        String objective = inputImage.getObjective();
        if (objective==null) {
            throw new IllegalArgumentException("Input image has no objective");
        }

        Date captureDate = inputImage.getCaptureDate();
        if (captureDate==null) {
            throw new IllegalArgumentException("Input image has no captureDate");
        }

        String imageSize = inputImage.getImageSize();
        if (imageSize==null) {
            throw new IllegalArgumentException("Input image has no imageSize");
        }

        File file = new File(filepath);
        String macroArgs = getDistortionCorrectorArgs(file,
                serviceFolder.toString(), microscope, objective,
                dcDateTimeFormat.format(captureDate), imageSize, fieldsFilepath);

        Path targetFilepath = serviceFolder.resolve(convertLsmToTargetName(file.getName()));

        return inline(fijiMacroService).process(getContext(sd,"Copy primary image"),
                new ServiceArg("-macroPath", macroFilepath),
                new ServiceArg("-macroArgs", macroArgs))
                .thenApply((JacsServiceResult<Void> result) -> {
                    // Create a new image based on the primary image
                    WorkflowImage outputImage = new WorkflowImage(inputImage);
                    // Clear existing files
                    outputImage.setFiles(new HashMap<>());
                    // Add the temporary stack
                    DomainUtils.setFilepath(outputImage, FileType.LosslessStack, targetFilepath.toString());
                    // Ensure it gets deleted once the workflow is done
                    outputImage.setDeleteOnExit(Sets.newHashSet(FileType.LosslessStack));
                    // Return the single result
                    return new JacsServiceResult<>(sd, outputImage);
                });
    }

    public String getDistortionCorrectorArgs(File file, String outDir, String microscope, String objective,
                                             String captureDate, String imageSize, String jsonDir) {
        String Xdimension = imageSize.split("\\.")[0];
        return "\""+file.getParent()+"/,"+file.getName()+","+outDir+"/," + microscope + "," +
                objective + "," + captureDate + "," + Xdimension + "," + jsonDir+"\"";
    }

    private String convertLsmToTargetName(String lsmName) {
        if (lsmName.toLowerCase().endsWith(LSM_SUFFIX)) {
            return lsmName.substring(0, lsmName.lastIndexOf(LSM_SUFFIX)) + V3DRAW_SUFFIX;
        }
        else {
            throw new IllegalArgumentException("Input image is not an LSM");
        }
    }
}
