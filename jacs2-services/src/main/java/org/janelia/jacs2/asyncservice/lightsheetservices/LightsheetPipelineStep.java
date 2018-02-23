package org.janelia.jacs2.asyncservice.lightsheetservices;

import com.beust.jcommander.Parameter;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Service for running a single lightsheet processing step.
 *
 * @author David Ackerman
 */
@Named("lightsheetPipeline")
public class LightsheetPipelineStep extends AbstractExeBasedServiceProcessor<List<File>> {

    static class LightsheetPipelineArgs extends ServiceArgs {
        @Parameter(names = "-stepName", description = "Input directory containing octree", required = true)
        String stepName;
        @Parameter(names = "-jsonFile", description = "Output directory for octree", required = true)
        String jsonFile;
        @Parameter(names = "-numTimePoints", description = "Number of tree levels", required = false)
        String numTimePoints = "N/A";
        @Parameter(names = "-timePointsPerJob", description = "Number of tree levels", required = false)
        String timePointsPerJob = "N/A";
    }

    private final String executable;

    @Inject
    LightsheetPipelineStep(ServiceComputationFactory computationFactory,
                           JacsServiceDataPersistence jacsServiceDataPersistence,
                           @Any Instance<ExternalProcessRunner> serviceRunners,
                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                           @PropertyValue(name = "LightsheetPipeline.Bin.Path") String executable,
                           JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                           @ApplicationProperties ApplicationConfig applicationConfig,
                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.executable = executable;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(LightsheetPipelineStep.class, new LightsheetPipelineArgs());
    }

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
        if (args.stepName.contains("local") ) {
            //Then should run on one node
            ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
            ScriptWriter scriptWriter = externalScriptCode.getCodeWriter();
            scriptWriter.add(args.stepName);
            scriptWriter.add(args.jsonFile);
            scriptWriter.close();
            return Arrays.asList(externalScriptCode);
        } else {
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

}
