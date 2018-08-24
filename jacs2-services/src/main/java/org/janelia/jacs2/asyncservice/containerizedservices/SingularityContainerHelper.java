package org.janelia.jacs2.asyncservice.containerizedservices;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

class SingularityContainerHelper {

    private static final String DEFAULT_IMAGE_EXT = ".simg";

    static <A extends AbstractSingularityContainerArgs> BiFunction<A, String, Path> getLocalContainerImageMapper() {
        return (A args, String defaultContainerImagesDirPath) -> {
            Pair<Path, String> containerImageWithPath = getLocalContainerImage(args.containerLocation);
            if (containerImageWithPath.getLeft() != null) {
                // the configured container location is a local path so in this case simply take the given location.
                return containerImageWithPath.getLeft().resolve(containerImageWithPath.getRight());
            } else {
                String containerName = getContainerNameFromArgs(args).orElse(containerImageWithPath.getRight());
                Function<String, Path> containerImageDirMapper = containerImagesPath -> {
                    if (StringUtils.isNotBlank(containerImagesPath)) {
                        return Paths.get(containerImagesPath, containerName);
                    } else {
                        return Paths.get(containerName);
                    }
                };
                return getLocalImagesDir(args, defaultContainerImagesDirPath)
                        .map(containerImageDirMapper::apply)
                        .orElse(containerImageDirMapper.apply(""));
            }
        };
    }

    static <A extends AbstractSingularityContainerArgs> Optional<String> getLocalImagesDir(A args, String defaultContainerImagesDir) {
        if (StringUtils.isNotBlank(args.containerImagesDirectory)) {
            return Optional.of(args.containerImagesDirectory);
        } else if (StringUtils.isNotBlank(defaultContainerImagesDir)) {
            return Optional.of(defaultContainerImagesDir);
        } else {
            return Optional.empty();
        }
    }

    private static <A extends AbstractSingularityContainerArgs> Optional<String> getContainerNameFromArgs(AbstractSingularityContainerArgs args) {
        if (StringUtils.isNotBlank(args.containerName)) {
            return Optional.of(Paths.get(args.containerName).getFileName().toString());
        } else {
            return Optional.empty();
        }
    }

    /**
     * @param containerLocation
     * @return a pair of the container directory and the parsed out container name. Container directory is only set if
     * the container location is a local path
     */
    private static Pair<Path, String> getLocalContainerImage(String containerLocation) {
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

    private static String parseDockerHubOrSHubName(String containerLocation) {
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
    private static Pair<String, String> extractComponent(String s, char separator, Function<String, Pair<String, String>> missingComponentHandler) {
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

    static Optional<String> getRuntime(AbstractSingularityContainerArgs args) {
        if (StringUtils.isNotBlank(args.singularityRuntime)) {
            return Optional.of(args.singularityRuntime);
        } else {
            return Optional.empty();
        }
    }
}
