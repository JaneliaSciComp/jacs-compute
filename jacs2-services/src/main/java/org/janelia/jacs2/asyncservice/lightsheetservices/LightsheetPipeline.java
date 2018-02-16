package org.janelia.jacs2.asyncservice.lightsheetservices;

import com.beust.jcommander.Parameter;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ProcessorHelper;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractFileListServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import java.util.ArrayList;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.FileFilter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
/**
 * Service for running a lightsheet processing step
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("lightsheetPipeline")
public class LightsheetPipeline extends AbstractExeBasedServiceProcessor<List<File>> {

    static class LightsheetPipelineArgs extends ServiceArgs {
        @Parameter(names = "-stepName", description = "Input directory containing octree", required = true)
        String stepName;
        @Parameter(names = "-jsonFile", description = "Output directory for octree", required = true)
        String jsonFile;
        @Parameter(names = "-numTimePoints", description = "Number of tree levels", required = false)
        String numTimePoints = "N/A";
        @Parameter(names = "-timePointsPerJob", description = "Number of tree levels", required = false)
        String timePointsPerJob = "N/A";
       // @Parameter(names = "-jsonDirectory", description = "Directory with JSON files", required = true)
       // String jsonDirectory;
    }

    private final String executable;

    @Inject
    LightsheetPipeline(ServiceComputationFactory computationFactory,
                       JacsServiceDataPersistence jacsServiceDataPersistence,
                       @Any Instance<ExternalProcessRunner> serviceRunners,
                       @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                       @PropertyValue(name = "LightsheetPipeline.Bin.Path") String executable,
                       JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                       @ApplicationProperties ApplicationConfig applicationConfig,
                       Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.executable = getFullExecutableName(executable);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(LightsheetPipeline.class, new LightsheetPipelineArgs());
    }

  /*  @Override
    public ServiceResultHandler<List<File>> getResultHandler() {
        return new AbstractFileListServiceResultHandler() {

            private boolean verifyOctree(File dir) {

                boolean checkChanFile = false;
                for(File file : dir.listFiles((FileFilter)null)) {
                    if (file.isDirectory()) {
                        try {
                            Integer.parseInt(file.getName());
                            if (!verifyOctree(file)) return false;
                        }
                        catch (NumberFormatException e) {
                            // Ignore dirs which are not numbers
                        }
                    }
                    else {
                        if (file.getName().startsWith("block") && file.getName().endsWith(".ktx")) {
                            checkChanFile = true;
                        }
                    }
                }
                if (!checkChanFile) return false;
                return true;
            }

            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                File outputDir = new File(getArgs(depResults.getJacsServiceData()).output);
                if (!outputDir.exists()) return false;
                if (!verifyOctree(outputDir)) return false;
                return true;
            }

            @Override
            public List<File> collectResult(JacsServiceResult<?> depResults) {
                LightsheetPipelineArgs args = getArgs(depResults.getJacsServiceData());
                Path outputDir = getOutputDir(args);
                return FileUtils.lookupFiles(outputDir, 100, "glob:*.ktx")
                        .map(Path::toFile)
                        .collect(Collectors.toList());
            }
        };
    } */

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        LightsheetPipelineArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter externalScriptWriter = externalScriptCode.getCodeWriter();
        createScript(args, externalScriptWriter);
        externalScriptWriter.close();
        return externalScriptCode;
    }

    private void createScript(LightsheetPipelineArgs args, ScriptWriter scriptWriter) {
        //scriptWriter.read("JSONDIRECTORY");
        scriptWriter.read("STEPNAME");
        scriptWriter.read("JSONFILE");
        if ( args.stepName.contains("cluster")) //Then should run on one node
        {
            scriptWriter.read("TIMEPOINTSPERJOB");
            scriptWriter.read("JOBNUMBER");
        }
        scriptWriter.addWithArgs(getFullExecutableName(executable));
        scriptWriter.addArg("$STEPNAME");
        scriptWriter.addArg("$JSONFILE");
        if ( args.stepName.contains("cluster") ) //Then should run on one node
        {
            scriptWriter.addArg("$TIMEPOINTSPERJOB");
            scriptWriter.addArg("$JOBNUMBER");
        }
        scriptWriter.endArgs();
    }

    @Override
    protected List<ExternalCodeBlock> prepareConfigurationFiles(JacsServiceData jacsServiceData) {
        LightsheetPipelineArgs args = getArgs(jacsServiceData);
        if ( args.stepName.contains("local") ) //Then should run on one node
        {
            ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
            ScriptWriter scriptWriter = externalScriptCode.getCodeWriter();
            scriptWriter.add(args.stepName);
            scriptWriter.add(args.jsonFile);
            scriptWriter.close();
            return Arrays.asList(externalScriptCode);
        }
        else {
            List<ExternalCodeBlock> blocks = new ArrayList<>();
            Integer numJobs = (int) Math.ceil(Double.parseDouble(args.numTimePoints) / Integer.valueOf(args.timePointsPerJob));
            for (int jobNumber = 1; jobNumber <= numJobs; jobNumber++) {
                parallelizeLightsheetStep(blocks, args, jobNumber);
            }
            return blocks;
        }
    }

    /**
     * Walks the input octree and generates a number of scripts to process it. This was simply
     * transliterated from the Python version (ktx/src/tools/cluster/create_cluster_scripts.py),
     * which was intended for very large MouseLight data. It may not be optimal for smaller data.
     */
    private void parallelizeLightsheetStep(List<ExternalCodeBlock> blocks, LightsheetPipelineArgs args, Integer jobNumber) {

        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter scriptWriter = externalScriptCode.getCodeWriter();
        scriptWriter.add(args.stepName);
        scriptWriter.add(args.jsonFile);
        scriptWriter.add(args.timePointsPerJob);
        scriptWriter.add(jobNumber.toString());
        scriptWriter.close();
        blocks.add(externalScriptCode);
    }

    @Override
    protected void prepareResources(JacsServiceData jacsServiceData) {
        // This doesn't need much memory, because it only processes a single tile at a time.
        ProcessorHelper.setCPUType(jacsServiceData.getResources(),"broadwell");
        LightsheetPipelineArgs args = getArgs(jacsServiceData);
        if ( args.stepName.contains("local") ) {
            ProcessorHelper.setRequiredSlots(jacsServiceData.getResources(), 32);
        }
        else if (args.stepName.contains("CS") ) {
            ProcessorHelper.setRequiredSlots(jacsServiceData.getResources(), 8);
        }
        else{
            ProcessorHelper.setRequiredSlots(jacsServiceData.getResources(), 2);
        }
        ProcessorHelper.setSoftJobDurationLimitInSeconds(jacsServiceData.getResources(), 5*60); // 5 minutes
        ProcessorHelper.setHardJobDurationLimitInSeconds(jacsServiceData.getResources(), 12*60*60); // 1 hour
    }

    private LightsheetPipelineArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new LightsheetPipelineArgs());
    }

    /*private Path getOutputDir(LightsheetPipelineArgs args) {
        return Paths.get(args.output).toAbsolutePath();
    }*/
}
