package org.janelia.jacs2.asyncservice.sample;

import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.common.resulthandlers.EmptyServiceResultHandler;
import org.janelia.jacs2.asyncservice.exceptions.MissingGridResultException;
import org.janelia.jacs2.asyncservice.sample.aux.LSMMetadata;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.asyncservice.utils.Task;
import org.janelia.jacs2.cdi.SingularityAppFactory;
import org.janelia.model.access.domain.DomainUtils;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.workflow.WorkflowImage;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Named("lsmMetadata")

@Service(description="Extract the metadata from the given LSM and parse it. Output the path to the raw JSON, as well the parsed attributes populated inside a Workflow image.")

@ServiceInput(name="lsm",
        type=WorkflowImage.class,
        description="Input LSM image")

@ServiceResult(
        name="outputImage",
        type=WorkflowImage.class,
        description="Output image with attached secondary data")

public class LSMMetadataService extends AbstractExeBasedServiceProcessor2<WorkflowImage> {

    @Inject
    private SingularityAppFactory singularityApps;

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(LSMMetadataService.class);
    }

    @Override
    public ServiceResultHandler<WorkflowImage> getResultHandler() {
        return new EmptyServiceResultHandler<>();
    }

    @Override
    protected void createScript(JacsServiceData jacsServiceData, ScriptWriter scriptWriter) {
        WorkflowImage lsm = (WorkflowImage) jacsServiceData.getDictionaryArgs().get("lsm");
        String lsmFilepath = DomainUtils.getFilepath(lsm, FileType.LosslessStack);
        String prefix = FileUtils.getFileName(lsmFilepath);
        String metadataFile  = prefix+".json";

        try {
            JacsServiceFolder serviceWorkingFolder = getWorkingDirectory(jacsServiceData);
            String workdir = serviceWorkingFolder.toString();

            scriptWriter.add("set -ex");
            scriptWriter.setVar("LSM_FILEPATH", lsmFilepath);
            scriptWriter.setVar("OUTPUT_DIR", workdir);
            scriptWriter.setVar("METADATA_FILE", metadataFile);
            scriptWriter.addWithArgs("cd").endArgs("$OUTPUT_DIR");

            // Extract LSM metadata
//            SingularityApp dumpPerlApp = singularityApps.getSingularityApp("informatics_perl", "dump_perl");
            SingularityApp dumpJsonApp = singularityApps.getSingularityApp("informatics_perl", "dump_json");
            List<String> externalDirs = Arrays.asList("$LSM_FILEPATH", serviceWorkingFolder.toString());
//            scriptWriter.add(dumpPerlApp.getSingularityCommand(externalDirs, Arrays.asList("$LSM_FILEPATH")));
            scriptWriter.add(dumpJsonApp.getSingularityCommand(externalDirs, Arrays.asList("$LSM_FILEPATH", ">$METADATA_FILE")));

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected JacsServiceResult<WorkflowImage> postProcessing(JacsServiceResult<WorkflowImage> sr) throws Exception {

        JacsServiceData jacsServiceData = sr.getJacsServiceData();

        WorkflowImage lsm = (WorkflowImage)jacsServiceData.getDictionaryArgs().get("lsm");
        String lsmFilepath = DomainUtils.getFilepath(lsm, FileType.LosslessStack);
        String prefix = FileUtils.getFileName(lsmFilepath);
        String metadataFile  = prefix+".json";

        JacsServiceFolder serviceWorkingFolder = getWorkingDirectory(jacsServiceData);
        File outputDir = serviceWorkingFolder.toFile();
        File outputFile = new File(outputDir, metadataFile);
        if (!outputFile.exists()) {
            throw new MissingGridResultException(outputDir.getAbsolutePath(), "Missing output file "+outputFile);
        }

        WorkflowImage outputImage = new WorkflowImage(lsm);
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

        jacsServiceData.setSerializableResult(outputImage);
        jacsServiceDataPersistence.updateServiceResult(jacsServiceData);
        return new JacsServiceResult<>(jacsServiceData, outputImage);
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
