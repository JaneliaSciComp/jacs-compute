package org.janelia.jacs2.asyncservice.neuronservices;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

@Named("neuronSeparation")
public class NeuronSeparationProcessor extends AbstractNeuronSeparationProcessor {

    @Inject
    NeuronSeparationProcessor(ServiceComputationFactory computationFactory,
                              JacsServiceDataPersistence jacsServiceDataPersistence,
                              @Any Instance<ExternalProcessRunner> serviceRunners,
                              @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                              @PropertyValue(name = "NeuronSeparator.Script.Path") String executable,
                              @PropertyValue(name = "NeuronSeparator.Library.Path") String libraryPath,
                              JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                              @ApplicationProperties ApplicationConfig applicationConfig,
                              Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, executable, libraryPath, jacsJobInstanceInfoDao, applicationConfig, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(NeuronSeparationProcessor.class, new NeuronSeparationArgs());
    }

    @Override
    protected void prepareProcessing(JacsServiceData jacsServiceData) {
        super.prepareProcessing(jacsServiceData);
        NeuronSeparationArgs args = getArgs(jacsServiceData);
        try {
            Path outputDir = getOutputDir(args);
            Files.createDirectories(outputDir);
        } catch (IOException e) {
            throw new ComputationException(jacsServiceData, e);
        }
    }

    protected NeuronSeparationArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new NeuronSeparationArgs());
    }

}
