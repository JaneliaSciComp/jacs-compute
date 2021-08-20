package org.janelia.jacs2.asyncservice.containerizedservices;

import com.google.common.collect.ImmutableSet;
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
import org.janelia.model.service.ProcessingLocation;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Named("simpleRunSingularityContainer")
public class SimpleRunSingularityContainerProcessor extends AbstractContainerProcessor<SingularityRunContainerArgs, Void> {

    @Inject
    SimpleRunSingularityContainerProcessor(ServiceComputationFactory computationFactory,
                                           JacsServiceDataPersistence jacsServiceDataPersistence,
                                           @Any Instance<ExternalProcessRunner> serviceRunners,
                                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                           @PropertyValue(name = "Singularity.Bin.Path") String singularityExecutable,
                                           JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                                           @ApplicationProperties ApplicationConfig applicationConfig,
                                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, singularityExecutable, jacsJobInstanceInfoDao, applicationConfig, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SimpleRunSingularityContainerProcessor.class, new SingularityRunContainerArgs());
    }

    @Override
    boolean createScript(JacsServiceData jacsServiceData, SingularityRunContainerArgs args, ScriptWriter scriptWriter) {
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
        scriptWriter.addWithArgs(getRuntime((args)));
        if (CollectionUtils.isNotEmpty(args.runtimeArgs)) {
            args.runtimeArgs.forEach(scriptWriter::addArg);
        }
        scriptWriter.addArg("run");
        if (StringUtils.isNotBlank(args.appName)) {
            scriptWriter.addArgs("--app", args.appName);
        }
        String scratchDir = serviceScratchDir(jacsServiceData);
        String bindPaths = args.bindPathsAsString(
                ImmutableSet.<BindPath>builder()
                        .addAll(args.bindPaths)
                        .add(new BindPath().setSrcPath(scratchDir))
                        .build());
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
        scriptWriter.addArg(args.containerLocation);
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

    @Override
    protected List<ExternalCodeBlock> prepareConfigurationFiles(JacsServiceData jacsServiceData) {
        SingularityRunContainerArgs args = getArgs(jacsServiceData);
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

    SingularityRunContainerArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SingularityRunContainerArgs());
    }
}
