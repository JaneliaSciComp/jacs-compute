package org.janelia.jacs2.asyncservice.containerizedservices;

import com.beust.jcommander.Parameter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.VoidServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;

@Named("runSingularityContainer")
public class RunSingularityContainerProcessor extends AbstractSingularityContainerProcessor<RunSingularityContainerProcessor.RunSingularityContainerArgs, Void> {

    enum ContainerOperation {
        run,
        exec
    }
    static class RunSingularityContainerArgs extends AbstractSingularityContainerArgs {
        @Parameter(names = "-op", description = "Singularity container operation {run (default) | exec}")
        ContainerOperation operation = ContainerOperation.run;
        @Parameter(names = "-appName", description = "Containerized application Name")
        String appName;
        @Parameter(names = "-bindPaths", description = "Container bind paths")
        List<String> bindPaths;
        @Parameter(names = "-overlay", description = "Container overlay")
        String overlay;
        @Parameter(names = "-enableNV", description = "Enable NVidia support")
        boolean enableNV;
        @Parameter(names = "-initialPwd", description = "Initial working directory inside the container")
        String initialPwd;
        @Parameter(names = "-appArgs", description = "Containerized application arguments")
        List<String> appArgs;

        RunSingularityContainerArgs() {
            super("Service that runs a singularity container");
        }
    }

    @Inject
    RunSingularityContainerProcessor(ServiceComputationFactory computationFactory,
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
        return ServiceArgs.getMetadata(RunSingularityContainerProcessor.class, new RunSingularityContainerArgs());
    }

    @Override
    public ServiceResultHandler<Void> getResultHandler() {
        return new VoidServiceResultHandler();
    }

    @Override
    void createScript(RunSingularityContainerArgs args, ScriptWriter scriptWriter) {
        scriptWriter
                .addWithArgs(getRuntime((args)))
                .addArg(args.operation.name());
        if (StringUtils.isNotBlank(args.appName)) {
            scriptWriter.addArgs("--app", args.appName);
        }
        String bindPaths = args.bindPaths.stream().filter(StringUtils::isNotBlank).reduce((s1, s2) -> s1.trim() + "," + s2.trim()).orElse("");
        if (StringUtils.isNotBlank(bindPaths)) {
            scriptWriter.addArgs("--bind", bindPaths);
        }
        if (StringUtils.isNotBlank(args.overlay)) {
            scriptWriter.addArgs("--overlay", args.overlay);
        }
        if (args.enableNV) {
            scriptWriter.addArg("--nv");
        }
        if (StringUtils.isNotBlank(args.initialPwd)) {
            scriptWriter.addArgs("--pwd", args.initialPwd);
        }
        scriptWriter.addArg(getLocalContainerImage(args).toString());
        if (CollectionUtils.isNotEmpty(args.appArgs)) {
            args.appArgs.forEach(scriptWriter::addArg);
        }
        scriptWriter.endArgs();
    }

    @Override
    RunSingularityContainerArgs createContainerArgs() {
        return new RunSingularityContainerArgs();
    }

}
