package org.janelia.jacs2.asyncservice.containerizedservices;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Named("simpleRunSingularityContainer")
public class SimpleRunSingularityContainerProcessor extends AbstractSingularityContainerProcessor<Void> {

    @Inject
    SimpleRunSingularityContainerProcessor(ServiceComputationFactory computationFactory,
                                           JacsServiceDataPersistence jacsServiceDataPersistence,
                                           @Any Instance<ExternalProcessRunner> serviceRunners,
                                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                           @PropertyValue(name = "Singularity.Bin.Path") String singularityExecutable,
                                           @PropertyValue(name = "Singularity.LocalImages.Path") String localSingularityImagesPath,
                                           JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                                           @ApplicationProperties ApplicationConfig applicationConfig,
                                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, singularityExecutable, localSingularityImagesPath, jacsJobInstanceInfoDao, applicationConfig, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SimpleRunSingularityContainerProcessor.class, new RunSingularityContainerArgs());
    }

    @Override
    void createScript(AbstractSingularityContainerArgs args, ScriptWriter scriptWriter) {
        RunSingularityContainerArgs runArgs = (RunSingularityContainerArgs) args;
        scriptWriter
                .addWithArgs(getRuntime((runArgs)))
                .addArg(runArgs.operation.name());
        if (StringUtils.isNotBlank(runArgs.appName)) {
            scriptWriter.addArgs("--app", runArgs.appName);
        }
        String bindPaths = runArgs.bindPathsAsString();
        if (StringUtils.isNotBlank(bindPaths)) {
            scriptWriter.addArgs("--bind", bindPaths);
        }
        if (StringUtils.isNotBlank(runArgs.overlay)) {
            scriptWriter.addArgs("--overlay", runArgs.overlay);
        }
        if (runArgs.enableNV) {
            scriptWriter.addArg("--nv");
        }
        if (StringUtils.isNotBlank(runArgs.initialPwd)) {
            scriptWriter.addArgs("--pwd", runArgs.initialPwd);
        }
        ContainerImage containerImage = getLocalContainerImage(runArgs);
        scriptWriter.addArg(containerImage.getLocalImagePath().toString());
        if (CollectionUtils.isNotEmpty(runArgs.appArgs)) {
            runArgs.appArgs.forEach(scriptWriter::addArg);
        }
        List<String> remainingArgs = runArgs.getRemainingArgs();
        if (CollectionUtils.isNotEmpty(remainingArgs)) {
            remainingArgs.stream().filter(StringUtils::isNotBlank).forEach(scriptWriter::addArg);
        }
        scriptWriter.endArgs();
    }

    @Override
    protected List<ExternalCodeBlock> prepareConfigurationFiles(JacsServiceData jacsServiceData) {
        RunSingularityContainerArgs args = getArgs(jacsServiceData);
        return args.batchJobArgs.stream()
                .map(instanceArgs -> {
                    ExternalCodeBlock instanceConfig = new ExternalCodeBlock();
                    ScriptWriter configWriter = instanceConfig.getCodeWriter();
                    configWriter.add(instanceArgs);
                    return instanceConfig;
                })
                .collect(Collectors.toList());
    }

    RunSingularityContainerArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new RunSingularityContainerArgs());
    }

}
