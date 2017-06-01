package org.janelia.jacs2.asyncservice.neuronservices;

import com.beust.jcommander.JCommander;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ThrottledProcessesQueue;
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

@Named("neuronSeparation")
public class NeuronSeparationProcessor extends AbstractNeuronSeparationProcessor {

    @Inject
    NeuronSeparationProcessor(ServiceComputationFactory computationFactory,
                              JacsServiceDataPersistence jacsServiceDataPersistence,
                              @Any Instance<ExternalProcessRunner> serviceRunners,
                              @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                              @PropertyValue(name = "NeuronSeparator.Script.Path") String executable,
                              @PropertyValue(name = "NeuronSeparator.Library.Path") String libraryPath,
                              ThrottledProcessesQueue throttledProcessesQueue,
                              @ApplicationProperties ApplicationConfig applicationConfig,
                              Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, executable, libraryPath, throttledProcessesQueue, applicationConfig, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(NeuronSeparationProcessor.class, new NeuronSeparationArgs());
    }

    @Override
    protected JacsServiceData prepareProcessing(JacsServiceData jacsServiceData) {
        NeuronSeparationArgs args = getArgs(jacsServiceData);
        try {
            Path outputDir = getOutputDir(args);
            Files.createDirectories(outputDir);
        } catch (IOException e) {
            throw new ComputationException(jacsServiceData, e);
        }
        return super.prepareProcessing(jacsServiceData);
    }

    protected NeuronSeparationArgs getArgs(JacsServiceData jacsServiceData) {
        NeuronSeparationArgs args = new NeuronSeparationArgs();
        new JCommander(args).parse(jacsServiceData.getArgsArray());
        return args;
    }

}
