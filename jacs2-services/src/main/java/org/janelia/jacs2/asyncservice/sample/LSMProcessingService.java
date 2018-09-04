package org.janelia.jacs2.asyncservice.sample;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.common.resulthandlers.EmptyServiceResultHandler;
import org.janelia.jacs2.asyncservice.exceptions.MissingGridResultException;
import org.janelia.jacs2.asyncservice.sample.helpers.FileDiscoveryHelperNG;
import org.janelia.jacs2.asyncservice.sample.helpers.SampleHelper;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.asyncservice.utils.ScriptUtils;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.asyncservice.utils.Task;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.utils.ArchiveUtils;
import org.janelia.model.access.domain.ChanSpecUtils;
import org.janelia.model.access.domain.DomainUtils;
import org.janelia.model.access.domain.FijiColor;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.sample.FileGroup;
import org.janelia.model.domain.sample.LSMSummaryResult;
import org.janelia.model.domain.workflow.WorkflowImage;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

@Named("lsmProcessing")

@Service(description="Given an LSM, extract its metadata into a file, and generate MIP/movies for it")

@ServiceInput(name="lsm",
        type=WorkflowImage.class,
        description="Primary LSM image")

@ServiceResult(
        name="outputImage",
        type=LSMSummaryResult.class,
        description="LSM secondary data")

public class LSMProcessingService extends AbstractExeBasedServiceProcessor2<LSMSummaryResult> {

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

    @Inject @PropertyValue(name = "Executables.ModuleBase")
    private String executablesDir;

    @Inject @PropertyValue(name = "service.DefaultScratchDir")
    private String scratchDir;

    @Inject @PropertyValue(name = "FFMPEG.Bin.Path")
    private String ffmpegExecutable;

    private final String options = "mips:movies:legends:bcomp";
    private final String resultName = "LSM Summary Result";

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(LSMProcessingService.class);
    }

    @Override
    public ServiceResultHandler<LSMSummaryResult> getResultHandler() {
        return new EmptyServiceResultHandler<>();
    }

    @Override
    protected void createScript(JacsServiceData jacsServiceData, ScriptWriter scriptWriter) {

        WorkflowImage lsm = (WorkflowImage)jacsServiceData.getDictionaryArgs().get("lsm");
        String lsmFilepath = DomainUtils.getFilepath(lsm, FileType.LosslessStack);

        try {
            JacsServiceFolder serviceWorkingFolder = getWorkingDirectory(jacsServiceData);
            String workdir = serviceWorkingFolder.toString();

            // Set up variables
            scriptWriter.add("set -e")
                    .setVar("LSM_FILEPATH", lsmFilepath)
                    .setVar("OUTPUT_DIR", workdir)
                    .addWithArgs("cd").endArgs("$OUTPUT_DIR");

            // Init virtual framebuffer
            scriptWriter
                    .setVar("START_PORT", "`shuf -i 5000-6000 -n 1`")
                    .addWithArgs(". "+initXvfbPath).endArgs("$START_PORT");

            // Create temp dir so that large temporary avis are not created on the network drive
            ScriptUtils.createTempDir("cleanTemp", scratchDir, scriptWriter);

            // Combine the exit handlers
            scriptWriter
                    .add("function exitHandler() { cleanXvfb; cleanTemp; }")
                    .add("trap exitHandler EXIT");

            // Run FIJI
            String outputDir = "$TEMP_DIR";
            String macroPath = macroDir+"/"+macroFilename;
            scriptWriter
                    .traceOn()
                    .addWithArgs(fijiExecutable)
                    .addArg("-macro").addArg(macroPath)
                    .addArg(String.join(",", getMacroArgs(lsm, outputDir)))
                    .endArgs("&")
                    .traceOff();

            // Monitor Fiji and take periodic screenshots, killing it eventually
            scriptWriter.setVar("fpid", "$!");
            scriptWriter.addWithArgs(". "+monitorXvfbPath).addArg("$XVFB_PORT").addArg("$fpid").endArgs(getTimeoutInSeconds(jacsServiceData)+"");

            // Encode avi movies as mp4 and delete the input avi's
            String hcmd = getFormattedH264ConvertCommand("$fin", "$fout", false);
            scriptWriter
                    .add("cd $TEMP_DIR")
                    .add("for fin in *.avi; do")
                    .add("    fout=${fin%.avi}.mp4")
                    .add("    "+ hcmd + " && rm $fin")
                    .add("done");

            // Move everything to the final output directory
            scriptWriter
                    .traceOn()
                    .add("cp $TEMP_DIR/*.png $OUTPUT_DIR || true")
                    .add("cp $TEMP_DIR/*.mp4 $OUTPUT_DIR || true")
                    .add("cp $TEMP_DIR/*.properties $OUTPUT_DIR || true")
                    .traceOff();

        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String getMacroArgs(WorkflowImage lsm, String outputDir) {

        String filepath = DomainUtils.getFilepath(lsm, FileType.LosslessStack);
        String prefix = getLSMPrefix(lsm);
        String chanSpec = lsm.getChannelSpec();
        String chanColors = lsm.getChannelColors();
        String area = lsm.getAnatomicalArea();

        if (chanSpec==null) {
            throw new IllegalStateException("Channel specification attribute is null for LSM id="+lsm.getId());
        }

        // Attempt to use the colors stored in the LSM
        String colorspec = "";
        String divspec = "";
        if (chanColors!=null) {
            List<String> colors = Task.listOfStringsFromCsvString(chanColors);

            int i = 0;
            for(String hexColor : colors) {
                if (i>=chanSpec.length()) {
                    logger.warn("More colors ('"+chanColors+"') than channels ('"+chanSpec+"') in LSM id="+lsm.getId());
                    break;
                }
                char type = chanSpec.charAt(i);
                FijiColor fc = ChanSpecUtils.getColorCode(hexColor, type);
                colorspec += fc.getCode();
                divspec += fc.getDivisor();
                i++;
            }
        }

        // If there are any uncertainties, default to RGB1
        if (StringUtils.isEmpty(colorspec) || colorspec.contains("?")) {
            String invalidColorspec = colorspec;
            colorspec = ChanSpecUtils.getDefaultColorSpec(chanSpec, "RGB", "1");
            divspec = chanSpec.replaceAll("r", "2").replaceAll("s", "1");
            logger.warn("LSM "+lsm.getId()+" has illegal color specification "+chanColors+
                    " (interpreted as '"+invalidColorspec+"'). Defaulting to "+colorspec);
        }

        logger.info("Input file: "+filepath);
        logger.info("  Area: "+area);
        logger.info("  Channel specification: "+chanSpec);
        logger.info("  Color specification: "+colorspec);
        logger.info("  Divisor specification: "+divspec);
        logger.info("  Output prefix: "+prefix);

        StringJoiner builder = new StringJoiner(",");
        builder.add(outputDir);
        builder.add(prefix).add("");
        builder.add("$LSM_FILEPATH").add("");
        // TODO: laser power and gain are always empty in JACSv1... is that a mistake?
        builder.add("").add("");
        builder.add(StringUtils.defaultIfBlank(chanSpec, ""));
        builder.add(StringUtils.defaultIfBlank(colorspec, ""));
        builder.add(StringUtils.defaultIfBlank(divspec, ""));
        builder.add(StringUtils.defaultIfBlank(options, ""));

        return builder.toString();
    }

    public String getFormattedH264ConvertCommand(String inputFile, String outputFile, boolean truncateToEvenSize) {
        String trunc = truncateToEvenSize? "-vf \"scale=trunc(iw/2)*2:trunc(ih/2)*2\" " : "";
        String FFMPEG_CMD = executablesDir+"/"+ffmpegExecutable;
        return FFMPEG_CMD+" -y -r 7 -i \""+inputFile+"\" -vcodec libx264 -b:v 2000000 -preset slow -tune film -pix_fmt yuv420p "+trunc+" \""+outputFile+"\"";
    }

    @Override
    protected JacsServiceResult<LSMSummaryResult> postProcessing(JacsServiceResult<LSMSummaryResult> sr) throws Exception {

        JacsServiceData jacsServiceData = sr.getJacsServiceData();

        WorkflowImage lsm = (WorkflowImage)jacsServiceData.getDictionaryArgs().get("lsm");
        String prefix = getLSMPrefix(lsm);

        JacsServiceFolder serviceWorkingFolder = getWorkingDirectory(jacsServiceData);
        File outputDir = serviceWorkingFolder.toFile();

        File[] files = outputDir.listFiles();

        // MP4 files are the copied to output dir.  The AVI files are not.  First, ensure there SHOULD be movies.
        if (options.toLowerCase().contains("movies")) {
            File[] mp4Files = FileUtils.getFilesWithSuffixes(outputDir, ".mp4");
            if (mp4Files.length == 0) {
                throw new MissingGridResultException(outputDir.getAbsolutePath(), "MP4 generation failed for "+outputDir);
            }
        }

        int count = 0;
        for(File file : files) {
            String filename = file.getName();
            if (filename.startsWith(prefix) && (filename.endsWith(".png") || filename.endsWith(".mp4"))) {
                count++;
            }
        }
        if (count==0) {
            throw new MissingGridResultException(outputDir.getAbsolutePath(), "No output files found for input "+prefix+" in "+outputDir);
        }

        LSMSummaryResult result = discoverFiles(jacsServiceData, outputDir);
        jacsServiceData.setSerializableResult(result);
        jacsServiceDataPersistence.updateServiceResult(jacsServiceData);
        return new JacsServiceResult<>(jacsServiceData, result);
    }

    public LSMSummaryResult discoverFiles(JacsServiceData jacsServiceData, File outputPath) throws Exception {

        String rootPath = outputPath.getAbsolutePath();
        WorkflowImage lsm = (WorkflowImage)jacsServiceData.getDictionaryArgs().get("lsm");
        String prefix = getLSMPrefix(lsm);

        SampleHelper sampleHelper = new SampleHelper(jacsServiceData, logger);
        FileDiscoveryHelperNG helper = new FileDiscoveryHelperNG(jacsServiceData.getOwnerKey(), logger);
        List<String> filepaths = helper.getFilepaths(rootPath);
        LSMSummaryResult result = new LSMSummaryResult();
        result.setFilepath(rootPath);

        List<FileGroup> groups = sampleHelper.createFileGroups(result.getFilepath(), filepaths);
        if (groups.size()!=1) {
            throw new MissingGridResultException(rootPath, "Expected single file group in "+rootPath);
        }

        for(FileGroup group : groups) {
            logger.info("Discovered update for group "+group.getKey());
            if (prefix.equals(group.getKey())) {
                for (FileType fileType : group.getFiles().keySet()) {
                    String filepath = DomainUtils.getFilepath(group, fileType);
                    DomainUtils.setFilepath(result, fileType, filepath);
                }
            }
            else {
                throw new IllegalStateException("Group with unexpected prefix found: "+group.getKey());
            }
        }

        File propertiesFile = new File(rootPath, prefix+".properties");
        if (propertiesFile.exists()) {
            Properties properties = new Properties();
            properties.load(new FileReader(propertiesFile));
            String brightnessCompensation = properties.getProperty("image.brightness.compensation");
            if (!StringUtils.isBlank(brightnessCompensation)) {
                logger.info("  Setting brightness compensation: "+brightnessCompensation);
                result.setBrightnessCompensation(brightnessCompensation);
            }
            else {
                throw new MissingGridResultException(rootPath, "Missing image.brightness.compensation in "+propertiesFile);
            }
        }
        else {
            throw new MissingGridResultException(rootPath, "Did not find properties file at "+propertiesFile);
        }

        return result;
    }

    private String getLSMPrefix(WorkflowImage lsm) {
        String filepath = DomainUtils.getFilepath(lsm, FileType.LosslessStack);
        String decompressedFilepath = ArchiveUtils.getDecompressedFilepath(filepath);
        return FileUtils.getFilePrefix(decompressedFilepath);
    }

    @Override
    protected Integer getRequiredMemoryInGB() {
        // TODO: This is a max. Most require a lot less. We need to characterize this based on objective or file size.
        return 50;
    }

    @Override
    protected Integer getHardRuntimeLimitSeconds() {
        return 60 * 60; // 1 hour
    }

    @Override
    protected Integer getSoftRuntimeLimitSeconds() {
        return 200; // Estimated by looking at some actual jobs in the wild
    }
}
