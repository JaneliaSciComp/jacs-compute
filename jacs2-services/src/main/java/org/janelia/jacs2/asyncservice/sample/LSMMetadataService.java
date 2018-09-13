package org.janelia.jacs2.asyncservice.sample;

import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor2;
import org.janelia.jacs2.asyncservice.exceptions.MissingGridResultException;
import org.janelia.jacs2.asyncservice.sample.aux.LSMMetadata;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.asyncservice.utils.Task;
import org.janelia.jacs2.cdi.SingularityAppFactory;
import org.janelia.model.access.domain.DomainUtils;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.workflow.WorkflowImage;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Named("lsmMetadata")

@Service(description="Extract the metadata from the given LSM and parse it. Output the path to the raw JSON, as well the parsed attributes populated inside a Workflow image.")

@ServiceInput(name="lsm",
        type=WorkflowImage.class,
        description="Input LSM image")

@ServiceOutput(
        name="outputImage",
        type=WorkflowImage.class,
        description="Output image with attached secondary data")

public class LSMMetadataService extends AbstractExeBasedServiceProcessor2<WorkflowImage> {

    @Inject
    private SingularityAppFactory singularityApps;

    @Override
    protected void createScript(ScriptWriter scriptWriter) throws IOException {

        Path serviceFolder = currentService.getServicePath();
        WorkflowImage lsm = (WorkflowImage) currentService.getInput("lsm");
        String lsmFilepath = DomainUtils.getFilepath(lsm, FileType.LosslessStack);
        String prefix = FileUtils.getFileName(lsmFilepath);
        Path metadataPath  = serviceFolder.resolve(prefix+".json");

        scriptWriter.add("set -ex");
        scriptWriter.setVar("LSM_FILEPATH", lsmFilepath);
        scriptWriter.setVar("OUTPUT_DIR", serviceFolder.toString());
        scriptWriter.setVar("METADATA_FILE", metadataPath.toString());
        scriptWriter.addWithArgs("cd").endArgs("$OUTPUT_DIR");

        // Extract LSM metadata
        SingularityApp dumpJsonApp = singularityApps.getSingularityApp("informatics_perl", "dump_json");
        List<String> externalDirs = Arrays.asList("$LSM_FILEPATH", serviceFolder.toString());
        scriptWriter.add(dumpJsonApp.getSingularityCommand(externalDirs, Arrays.asList("$LSM_FILEPATH", ">$METADATA_FILE")));
    }

    @Override
    protected WorkflowImage createResult() throws Exception {

        Path serviceFolder = currentService.getServicePath();
        WorkflowImage lsm = (WorkflowImage)currentService.getInput("lsm");
        String lsmFilepath = DomainUtils.getFilepath(lsm, FileType.LosslessStack);
        String prefix = FileUtils.getFileName(lsmFilepath);
        String metadataFile  = prefix+".json";

        File outputFile = serviceFolder.resolve(metadataFile).toFile();
        if (!outputFile.exists()) {
            throw new MissingGridResultException(outputFile.toString(), "Missing output file "+outputFile);
        }

        WorkflowImage outputImage = new WorkflowImage(lsm);
        outputImage.setId(lsm.getId());

        String jsonFilepath = outputFile.getAbsolutePath();
        logger.info("  Setting JSON Metadata: "+jsonFilepath);
        DomainUtils.setFilepath(outputImage, FileType.LsmMetadata, jsonFilepath);

        List<String> colors = new ArrayList<>();
        List<String> dyeNames = new ArrayList<>();
        File jsonFile = new File(jsonFilepath);

        try {
            LSMMetadata metadata = LSMMetadata.fromFile(jsonFile);
            for(LSMMetadata.Channel channel : metadata.getChannels()) {
                colors.add(channel.getColor());
                LSMMetadata.DetectionChannel detection = metadata.getDetectionChannel(channel);
                if (detection!=null) {
                    dyeNames.add(detection.getDyeName());
                }
                else {
                    dyeNames.add("Unknown");
                }
            }
        }
        catch (Exception e) {
            throw new Exception("Error parsing LSM metadata file: "+jsonFile,e);
        }

        if (!colors.isEmpty() && !org.janelia.jacs2.utils.StringUtils.areAllEmpty(colors)) {
            logger.info("  Setting LSM colors: "+colors);
            outputImage.setChannelColors(Task.csvStringFromCollection(colors));
        }

        if (!dyeNames.isEmpty() && !org.janelia.jacs2.utils.StringUtils.areAllEmpty(dyeNames)) {
            logger.info("  Setting LSM dyes: "+dyeNames);
            outputImage.setChannelDyeNames(Task.csvStringFromCollection(dyeNames));
        }

        return outputImage;
    }

    @Override
    protected Integer getRequiredMemoryInGB() {
        return 5;
    }

    @Override
    protected Integer getHardRuntimeLimitSeconds() {
        return 2 * 60; // 5 minutes max
    }

    @Override
    protected Integer getSoftRuntimeLimitSeconds() {
        return 60; // Estimated by looking at some actual jobs in the wild
    }
}
