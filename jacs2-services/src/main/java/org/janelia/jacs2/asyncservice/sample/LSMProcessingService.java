package org.janelia.jacs2.asyncservice.sample;

import com.beust.jcommander.Parameter;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor2;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.VoidServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.access.domain.DomainUtils;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.sample.LSMImage;
import org.janelia.model.domain.workflow.WorkflowImage;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;

@Named("lsmProcessing")

@Service(description="Given an LSM, extract its metadata into a file, and generate MIP/movies for it")

@ServiceInput(name="lsm",
        type=LSMImage.class,
        description="Primary LSM image")

@ServiceResult(
        name="outputImage",
        type=WorkflowImage.class,
        description="LSM secondary data")

public class LSMProcessingService extends AbstractExeBasedServiceProcessor2<Void> {

//    static class LSMMetadataArgs extends ServiceArgs {
//        @Parameter(names = "-inputPath", description = "Input LSM path", required = true)
//        String inputLSMFile;
//        @Parameter(names = "-outputPath", description = "Destination directory", required = true)
//        String outputPath;
//    }

    @Inject @PropertyValue(name = "InitXvfb.Path")
    private String initXvfbPath;

    @Inject @PropertyValue(name = "MonitorXvfb.Path")
    private String monitorXvfbPath;

    @Inject @PropertyValue(name = "Fiji.Bin.Path")
    private String fijiExecutable;

    @Inject @PropertyValue(name = "Fiji.Macro.Path")
    private String macroDir;

    @Inject @PropertyValue(name = "Fiji.BasicMIPsAndMovies")
    private String macroFilename;

    @Inject @PropertyValue(name = "service.DefaultScratchDir")
    private String scratchDir;


    private String mipOptions = "mips:movies:legends:bcomp";



    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(LSMProcessingService.class);
    }

    @Override
    public ServiceResultHandler<Void> getResultHandler() {
        return new VoidServiceResultHandler();
    }

    @Override
    protected void createScript(JacsServiceData jacsServiceData, ScriptWriter scriptWriter) {

        LSMImage lsm = (LSMImage)jacsServiceData.getDictionaryArgs().get("lsm");
        String lsmFilepath = DomainUtils.getFilepath(lsm, FileType.LosslessStack);

        try {
            JacsServiceFolder serviceWorkingFolder = getWorkingDirectory(jacsServiceData);
            String workdir = serviceWorkingFolder.toString();


            scriptWriter.add("LSM_FILEPATH=" + lsmFilepath);
            scriptWriter.addWithArgs("cd").addArg(workdir);

            //
            SingularityApp dumpPerlApp = new SingularityApp("informatics_perl", "dump_perl");
            SingularityApp dumpJsonApp = new SingularityApp("informatics_perl", "dump_json");

            // Extract LSM metadata
            List<String> externalDirs = Arrays.asList("$LSM_FILEPATH", serviceWorkingFolder.toString());
            scriptWriter.add(dumpPerlApp.getSingularityCommand(externalDirs, Arrays.asList("$LSM_FILEPATH")));
            scriptWriter.add(dumpJsonApp.getSingularityCommand(externalDirs, Arrays.asList("$LSM_FILEPATH")));

            // Init virtual framebuffer
            scriptWriter.setVar("START_PORT", "`shuf -i 5000-6000 -n 1`");
            scriptWriter.addWithArgs(". "+initXvfbPath).addArg("$DISPLAY_PORT");

            // Create temp directory
//            ScriptUtils.createTempDir("cleanTemp", workdir, scriptWriter);
//            scriptWriter.addWithArgs("cd").addArg("$TEMP_DIR");

            // Combine the exit handlers
            scriptWriter
                    .add("function exitHandler() { cleanXvfb; cleanTemp; }")
                    .add("trap exitHandler EXIT");




            // Run FIJI
            String macroPath = macroDir+"/"+macroFilename;
            scriptWriter
                    .addWithArgs(fijiExecutable)
                    .addArg("-macro").addArg(macroPath)
                    .addArg(String.join(",", macroArgs))
                    .addArg("&");

            // Monitor Fiji and take periodic screenshots, killing it eventually
            scriptWriter.setVar("fpid", "$!");
            scriptWriter.addWithArgs(". "+monitorXvfbPath).addArg("PORT").addArg("fpid").addArg(getTimeoutInSeconds(jacsServiceData)+"");

        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
