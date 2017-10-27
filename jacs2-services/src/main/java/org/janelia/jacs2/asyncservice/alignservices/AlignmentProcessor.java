package org.janelia.jacs2.asyncservice.alignservices;

import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ThrottledProcessesQueue;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

@Named("alignment")
public class AlignmentProcessor extends AbstractAlignmentProcessor {

    @Inject
    AlignmentProcessor(ServiceComputationFactory computationFactory,
                       JacsServiceDataPersistence jacsServiceDataPersistence,
                       @Any Instance<ExternalProcessRunner> serviceRunners,
                       @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                       @PropertyValue(name = "Alignment.Runner.Path") String alignmentRunner,
                       @PropertyValue(name = "Alignment.Scripts.Path") String alignmentScriptsDir,
                       @PropertyValue(name = "Alignment.Tools.Path") String toolsDir,
                       @PropertyValue(name = "Alignment.Config.Path") String alignmentConfigDir,
                       @PropertyValue(name = "Alignment.Templates.Path") String alignmentTemplatesDir,
                       @PropertyValue(name = "Alignment.Library.Path") String libraryPath,
                       ThrottledProcessesQueue throttledProcessesQueue,
                       @ApplicationProperties ApplicationConfig applicationConfig,
                       Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, alignmentRunner, alignmentScriptsDir, toolsDir, alignmentConfigDir, alignmentTemplatesDir, libraryPath, throttledProcessesQueue, applicationConfig, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(AlignmentProcessor.class, new AlignmentArgs());
    }

}
