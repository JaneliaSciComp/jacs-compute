package org.janelia.jacs2.asyncservice.containerizedservices;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.VoidServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.function.Function;

abstract class AbstractSingularityContainerProcessor<A extends AbstractSingularityContainerArgs, R> extends AbstractExeBasedServiceProcessor<R> {

    private static final String DEFAULT_IMAGE_EXT = ".simg";

    private final String singularityExecutable;
    private final String localSingularityImagesPath;

    @Inject
    AbstractSingularityContainerProcessor(ServiceComputationFactory computationFactory,
                                          JacsServiceDataPersistence jacsServiceDataPersistence,
                                          Instance<ExternalProcessRunner> serviceRunners,
                                          String defaultWorkingDir,
                                          String singularityExecutable,
                                          String localSingularityImagesPath,
                                          JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                                          ApplicationConfig applicationConfig,
                                          Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.singularityExecutable = singularityExecutable;
        this.localSingularityImagesPath = localSingularityImagesPath;
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        A args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        createScript(args, externalScriptCode.getCodeWriter());
        return externalScriptCode;
    }

    abstract void createScript(A args, ScriptWriter scriptWriter);

    @Override
    protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
        AbstractSingularityContainerArgs args = getArgs(jacsServiceData);
        if (args.noHttps()) {
            return ImmutableMap.of("SINGULARITY_NOHTTPS", "True");
        } else {
            return ImmutableMap.of();
        }
    }

    A getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), createContainerArgs());
    }

    abstract A createContainerArgs();

    Path getLocalImagesDir(A args) {
        if (StringUtils.isNotBlank(args.containerImagesDirectory)) {
            return Paths.get(args.containerImagesDirectory);
        } else {
            return Paths.get(localSingularityImagesPath);
        }
    }

    Path getLocalContainerImage(A args) {
        Pair<Path, String> containerImageWithPath = getLocalContainerImage(args.containerLocation);
        if (containerImageWithPath.getLeft() != null) {
            // the configured container location is a local path so in this case simply take the given location.
            return containerImageWithPath.getLeft().resolve(containerImageWithPath.getRight());
        } else {
            return getLocalImagesDir(args).resolve(getContainerNameOrDefault(args, containerImageWithPath.getRight()));
        }
    }

    private String getContainerNameOrDefault(A args, String defaultContainerName) {
        if (StringUtils.isNotBlank(args.containerName)) {
            return Paths.get(args.containerName).getFileName().toString();
        } else {
            return defaultContainerName;
        }
    }

    /**
     * @param containerLocation
     * @return a pair of the container directory and the parsed out container name. Container directory is only set if
     * the container location is a local path
     */
    private Pair<Path, String> getLocalContainerImage(String containerLocation) {
        if (StringUtils.isBlank(containerLocation)) {
            throw new IllegalArgumentException("Container location cannot be empty");
        }
        if (StringUtils.startsWithIgnoreCase(containerLocation, "shub://")) {
            return ImmutablePair.of(null, parseDockerHubOrSHubName(containerLocation.substring("shub://".length())));
        } else if (StringUtils.startsWithIgnoreCase(containerLocation, "docker://")) {
            return ImmutablePair.of(null, parseDockerHubOrSHubName(containerLocation.substring("docker://".length())));
        } else {
            // assume a path
            Path p = Paths.get(containerLocation);
            return ImmutablePair.of(p.getParent(), p.getFileName().toString());
        }
    }

    private String parseDockerHubOrSHubName(String containerLocation) {
        Pair<String, String> afterTagResult = extractComponent(containerLocation, ':',
                s -> ImmutablePair.of("", s) /* leave remaining unchanged */);
        Pair<String, String> afterNameResult = extractComponent(afterTagResult.getRight(), '/',
                s -> ImmutablePair.of(s, "") /* consume the entire remaining string */);
        Pair<String, String> afterCollectionResult = extractComponent(afterNameResult.getRight(), '/',
                s -> ImmutablePair.of(s, "") /* consume the entire remaining string */);

        StringBuilder resultBuilder = new StringBuilder();
        if (StringUtils.isNotEmpty(afterCollectionResult.getLeft())) {
            resultBuilder.append(afterCollectionResult.getLeft())
                    .append('-');
        }
        resultBuilder.append(afterNameResult.getLeft());
        if (StringUtils.isNotEmpty(afterTagResult.getLeft())) {
            resultBuilder.append('-')
                    .append(afterTagResult.getLeft());
        }
        resultBuilder.append(DEFAULT_IMAGE_EXT);
        return resultBuilder.toString();
    }

    /**
     * Extracts the component examining the string from right to left.
     * @param s
     * @param separator
     * @param missingComponentHandler - tells how to advance the position when the separator is not found.
     * @return a pair of the extracted component and the remaining string; the component always starts after the separator.
     */
    private Pair<String, String> extractComponent(String s, char separator, Function<String, Pair<String, String>> missingComponentHandler) {
        if (StringUtils.isEmpty(s)) {
            return ImmutablePair.of("", "");
        }
        int separatorPos = s.lastIndexOf(separator);
        if (separatorPos == -1) {
            return missingComponentHandler.apply(s);
        } else {
            String component = s.substring(separatorPos + 1);
            String remaining = s.substring(0, separatorPos);
            return ImmutablePair.of(component, remaining);
        }
    }

    String getRuntime(AbstractSingularityContainerArgs args) {
        if (StringUtils.isNotBlank(args.singularityRuntime)) {
            return args.singularityRuntime;
        } else {
            return getFullExecutableName(singularityExecutable);
        }
    }
}
