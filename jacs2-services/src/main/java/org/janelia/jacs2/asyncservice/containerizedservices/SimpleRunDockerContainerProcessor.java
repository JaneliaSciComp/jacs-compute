package org.janelia.jacs2.asyncservice.containerizedservices;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

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

@Named("simpleRunDockerContainer")
public class SimpleRunDockerContainerProcessor extends AbstractContainerProcessor<RunContainerArgs, Void> {

    @Inject
    SimpleRunDockerContainerProcessor(ServiceComputationFactory computationFactory,
                                      JacsServiceDataPersistence jacsServiceDataPersistence,
                                      @Any Instance<ExternalProcessRunner> serviceRunners,
                                      @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                      @PropertyValue(name = "Docker.Bin.Path") String dockerExecutable,
                                      JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                                      @ApplicationProperties ApplicationConfig applicationConfig,
                                      Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, dockerExecutable, jacsJobInstanceInfoDao, applicationConfig, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SimpleRunDockerContainerProcessor.class, createArgs());
    }

    boolean createScript(JacsServiceData jacsServiceData, RunContainerArgs args, ScriptWriter scriptWriter) {
        List<String> expandedArgsAtRuntime = args.getExpandedArgsAtRuntime();
        if (args.getCancelIfNoExpandedArgs() && expandedArgsAtRuntime.isEmpty()) {
            return false;
        }
        if (CollectionUtils.isNotEmpty(args.batchJobArgs)) {
            scriptWriter.add("read INSTANCE_ARGS");
        }
        if (!expandedArgsAtRuntime.isEmpty()) {
            scriptWriter.add("read EXPANDED_ARG");
        }
        scriptWriter.add("ulimit -c 0");
        scriptWriter
                .addWithArgs(getRuntime((args)))
                .addArg("run");
        String scratchDir = serviceScratchDir(jacsServiceData);
        Stream.concat(args.bindPaths.stream(), Stream.of(new BindPath().setSrcPath(scratchDir)))
                .filter(BindPath::isNotEmpty)
                .map(bindPath -> bindPath.asString(true)) // docker volume bindings always have both source and target
                .forEach(bindPath -> scriptWriter.addArgs("-v", bindPath));
        if (CollectionUtils.isNotEmpty(args.runtimeArgs)) {
            args.runtimeArgs.forEach(scriptWriter::addArg);
        }
        scriptWriter.addArg(getContainerLocation(args));
        if (StringUtils.isNotBlank(args.appName)) {
            scriptWriter.addArg(args.appName);
        }
        if (CollectionUtils.isNotEmpty(args.appArgs)) {
            args.appArgs.forEach(scriptWriter::addArg);
        }
        if (CollectionUtils.isNotEmpty(args.batchJobArgs)) {
            scriptWriter.addArg("${INSTANCE_ARGS}");
        }
        if (!expandedArgsAtRuntime.isEmpty()) {
            if (StringUtils.isNotBlank(args.expandedArgFlag)) {
                scriptWriter.addArg(args.expandedArgFlag);
            }
            scriptWriter.addArg("${EXPANDED_ARG}");
        }
        List<String> remainingArgs = args.getRemainingArgs();
        if (CollectionUtils.isNotEmpty(remainingArgs)) {
            remainingArgs.stream().filter(StringUtils::isNotBlank).forEach(scriptWriter::addArg);
        }
        scriptWriter.endArgs();
        return true;
    }

    private String getContainerLocation(RunContainerArgs args) {
        return StringUtils.removeStartIgnoreCase(args.containerLocation, "docker://");
    }

    @Override
    protected List<ExternalCodeBlock> prepareConfigurationFiles(JacsServiceData jacsServiceData) {
        RunContainerArgs args = getArgs(jacsServiceData);
        List<String> expandedArgsAtRuntime = args.getExpandedArgsAtRuntime();
        return args.batchJobArgs.stream()
                .flatMap(instanceArgs -> {
                    if (expandedArgsAtRuntime.isEmpty()) {
                        return Stream.of(ExternalCodeBlock.builder().add(instanceArgs).build());
                    } else {
                        return expandedArgsAtRuntime.stream()
                                .map(expandedArg -> ExternalCodeBlock.builder().add(instanceArgs).add(expandedArg).build());
                    }
                })
                .collect(Collectors.toList());
    }


    RunContainerArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), createArgs());
    }

    private RunContainerArgs createArgs() {
        return new RunContainerArgs("Service that runs a docker container");
    }
}
