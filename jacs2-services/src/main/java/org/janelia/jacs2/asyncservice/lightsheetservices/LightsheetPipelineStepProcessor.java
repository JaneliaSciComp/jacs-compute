package org.janelia.jacs2.asyncservice.lightsheetservices;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.ProcessorHelper;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
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
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.janelia.jacs2.utils.HttpUtils;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

/**
 * Service for running a single lightsheet processing step.
 *
 * @author David Ackerman
 */
@Named("lightsheetPipeline")
public class LightsheetPipelineStepProcessor extends AbstractExeBasedServiceProcessor<Void> {

    private static final String DEFAULT_CONFIG_OUTPUT_PATH = "";

    static class LightsheetPipelineArgs extends ServiceArgs {
        @Parameter(names = "-step", description = "Which pipeline step to run", required = true)
        LightsheetPipelineStep step;
        @Parameter(names = "-stepIndex", description = "Which step index is this")
        Integer stepIndex = 1;
        @Parameter(names = "-numTimePoints", description = "Number of time points")
        Integer numTimePoints = 1;
        @Parameter(names = "-timePointsPerJob", description = "Number of time points per job")
        Integer timePointsPerJob = 1;
        @Parameter(names = "-configAddress", description = "Address for accessing job's config json", required = true)
        String configAddress;
        @Parameter(names = "-configOutputPath", description = "Path for outputting json configs", required=false)
        String configOutputPath = DEFAULT_CONFIG_OUTPUT_PATH;
    }

    private final String executable;
    private final ObjectMapper objectMapper;

    @Inject
    LightsheetPipelineStepProcessor(ServiceComputationFactory computationFactory,
                                    JacsServiceDataPersistence jacsServiceDataPersistence,
                                    @Any Instance<ExternalProcessRunner> serviceRunners,
                                    @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                    @PropertyValue(name = "LightsheetPipeline.Bin.Path") String executable,
                                    JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                                    @ApplicationProperties ApplicationConfig applicationConfig,
                                    ObjectMapper objectMapper,
                                    Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.executable = executable;
        this.objectMapper = objectMapper;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(LightsheetPipelineStepProcessor.class, new LightsheetPipelineArgs());
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
        if (args.step.requiresMoreThanOneJob()) {
            // Then should run on one node
            scriptWriter.read("TIMEPOINTSPERJOB");
            scriptWriter.read("JOBNUMBER");
        }
        scriptWriter.addWithArgs(getFullExecutableName(executable));
        scriptWriter.addArg("$STEPNAME");
        scriptWriter.addArg("$JSONFILE");
        if ( args.step.requiresMoreThanOneJob() ) {
            // Then should run on one node
            scriptWriter.addArg("$TIMEPOINTSPERJOB");
            scriptWriter.addArg("$JOBNUMBER");
        }
        scriptWriter.endArgs();
    }

    @Override
    protected List<ExternalCodeBlock> prepareConfigurationFiles(JacsServiceData jacsServiceData) {
        LightsheetPipelineArgs args = getArgs(jacsServiceData);
        String jsonConfigFile = getJsonConfigFile(jacsServiceData, args);
        if (args.step.needsOnlyOneJob()) {
            // Then should run on one node
            ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
            ScriptWriter scriptWriter = externalScriptCode.getCodeWriter();
            scriptWriter.add(args.step.name());
            scriptWriter.add(jsonConfigFile);
            scriptWriter.close();
            return Arrays.asList(externalScriptCode);
        } else {
            List<ExternalCodeBlock> blocks = new ArrayList<>();
            int numTimePoints = args.numTimePoints;
            int timePointsPerJob = args.timePointsPerJob;
            if (numTimePoints < 1) numTimePoints = 1;
            if (timePointsPerJob < 1) timePointsPerJob = 1;

            int numJobs = (int) Math.ceil((double)numTimePoints / timePointsPerJob);
            for (int jobNumber = 1; jobNumber <= numJobs; jobNumber++) {
                parallelizeLightsheetStep(blocks, args, jobNumber, jsonConfigFile);
            }
            return blocks;
        }
    }

    //Each time point gets its own job, assigned based on the job number
    private void parallelizeLightsheetStep(List<ExternalCodeBlock> blocks, LightsheetPipelineArgs args, Integer jobNumber, String jsonConfigFile) {
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter scriptWriter = externalScriptCode.getCodeWriter();
        scriptWriter.add(args.step.name());
        scriptWriter.add(jsonConfigFile);
        scriptWriter.add(String.valueOf(args.timePointsPerJob));
        scriptWriter.add(jobNumber.toString());
        scriptWriter.close();
        blocks.add(externalScriptCode);
    }

    @Override
    protected void prepareResources(JacsServiceData jacsServiceData) {
        String cpuType = ProcessorHelper.getCPUType(jacsServiceData.getResources());
        if (StringUtils.isBlank(cpuType)) {
            ProcessorHelper.setCPUType(jacsServiceData.getResources(), "broadwell");
        }
        LightsheetPipelineArgs args = getArgs(jacsServiceData);
        ProcessorHelper.setRequiredSlots(jacsServiceData.getResources(), args.step.getRecommendedSlots());
        ProcessorHelper.setSoftJobDurationLimitInSeconds(jacsServiceData.getResources(), 5*60); // 5 minutes
        ProcessorHelper.setHardJobDurationLimitInSeconds(jacsServiceData.getResources(), 12*60*60); // 12 hours
    }

    private LightsheetPipelineArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new LightsheetPipelineArgs());
    }

    private String getJsonConfigFile(JacsServiceData jacsServiceData, LightsheetPipelineArgs args) {
        Map<String, Object> stepConfig = readJsonConfig(getJsonConfig(args.configAddress, args.step.name()));
        stepConfig.putAll(jacsServiceData.getDictionaryArgs()); // overwrite arguments that were explicitly passed by the user
        // write the final config file
        JacsServiceFolder serviceWorkingFolder = getWorkingDirectory(jacsServiceData);
        String fileName = "stepConfig_" + String.valueOf(args.stepIndex) + "_" + args.step.name() + ".json";
        File jsonConfigFile = serviceWorkingFolder.getServiceFolder(fileName).toFile();
        writeJsonConfig(stepConfig, jsonConfigFile);
        if (!args.configOutputPath.equals("")) {
            String[] addressParts = args.configAddress.split("/");
            String lightsheetJobID = addressParts[addressParts.length -1];
            Path configOutputPath = Paths.get(args.configOutputPath + "/" + lightsheetJobID + "/" + fileName);
            File configOutputPathFile = configOutputPath.toFile();
            writeJsonConfig(stepConfig, configOutputPathFile);
        }
        return jsonConfigFile.getAbsolutePath();
    }

    private Map<String, Object> readJsonConfig(InputStream inputStream) {
        try {
            return objectMapper.readValue(inputStream, new TypeReference<Map<String, Object>>() {});
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeJsonConfig(Map<String, Object> config, File configFile) {
        try {
            Files.createDirectories(configFile.getParentFile().toPath());
            objectMapper.writeValue(configFile, config);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // Creates json file from http call
    private InputStream getJsonConfig(String configAddress, String stepName) {
        Client httpclient = null;
        try {
            httpclient = HttpUtils.createHttpClient();
            WebTarget target = httpclient.target(configAddress);

            Response response = target.queryParam("stepName", stepName).request().get();
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new IllegalStateException(configAddress + " returned with " + response.getStatus());
            }
            return response.readEntity(InputStream.class);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpclient != null) {
                httpclient.close();
            }
        }
    }

}

