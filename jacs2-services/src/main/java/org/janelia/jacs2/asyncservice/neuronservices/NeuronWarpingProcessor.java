package org.janelia.jacs2.asyncservice.neuronservices;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ThrottledProcessesQueue;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

@Named("neuronWarping")
public class NeuronWarpingProcessor extends AbstractNeuronSeparationProcessor {

    static class NeuronWarpingArgs extends NeuronSeparationProcessor.NeuronSeparationArgs {
        @Parameter(names = "-consolidatedLabelFile", description = "Consolidated label file name", required = true)
        String consolidatedLabelFile;
    }

    @Inject
    NeuronWarpingProcessor(ServiceComputationFactory computationFactory,
                           JacsServiceDataPersistence jacsServiceDataPersistence,
                           @Any Instance<ExternalProcessRunner> serviceRunners,
                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                           @PropertyValue(name = "NeuronWarping.Script.Path") String executable,
                           @PropertyValue(name = "NeuronWarping.Library.Path") String libraryPath,
                           ThrottledProcessesQueue throttledProcessesQueue,
                           @ApplicationProperties ApplicationConfig applicationConfig,
                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, executable, libraryPath, throttledProcessesQueue, applicationConfig, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(NeuronWarpingProcessor.class, new NeuronWarpingArgs());
    }

    @Override
    protected JacsServiceData prepareProcessing(JacsServiceData jacsServiceData) {
        NeuronWarpingArgs args = getArgs(jacsServiceData);
        try {
            Path outputDir = getOutputDir(args);
            Files.createDirectories(outputDir);
            // copy consolidated labels as the underlying script looks for it even though it's not specified anywhere in the command line.
            Path consolidatedLabel = getConsolidatedLabelFile(args);
            Path workingConsolidatedLabel = FileUtils.getFilePath(outputDir, "ConsolidatedLabel", FileUtils.getFileExtensionOnly(consolidatedLabel));
            if (Files.notExists(workingConsolidatedLabel) || Files.size(workingConsolidatedLabel) == 0) {
                logger.info("Copy {} -> {}", consolidatedLabel, workingConsolidatedLabel);
                Files.copy(consolidatedLabel, workingConsolidatedLabel, StandardCopyOption.REPLACE_EXISTING);
            } else {
                logger.info("A copy of {} already exists at {}", consolidatedLabel, outputDir);
            }
        } catch (IOException e) {
            throw new ComputationException(jacsServiceData, e);
        }
        return super.prepareProcessing(jacsServiceData);
    }

    protected NeuronWarpingArgs getArgs(JacsServiceData jacsServiceData) {
        NeuronWarpingArgs args = new NeuronWarpingArgs();
        new JCommander(args).parse(jacsServiceData.getArgsArray());
        return args;
    }

    private Path getConsolidatedLabelFile(NeuronWarpingArgs args) {
        return Paths.get(args.consolidatedLabelFile);
    }
}
