package org.janelia.jacs2.asyncservice.containerizedservices;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

class SingularityContainerHelper {

    private static final String DEFAULT_IMAGE_EXT = ".simg";

    static <A extends PullSingularityContainerArgs> BiFunction<A, String, ContainerImage> getLocalContainerImageMapper() {
        return (A args, String defaultContainerImagesDirPath) -> {
            ContainerImage containerImage = getLocalContainerImage(args.containerLocation);
            if (containerImage.isLocalImage()) {
                // the configured container location is a local path so just return the container image
                return containerImage;
            } else {
                return getLocalImagesDir(args, defaultContainerImagesDirPath)
                        .filter(containerImagePath -> StringUtils.isNotBlank(containerImagePath))
                        .map(containerImagePath -> new ContainerImage(containerImage.protocol,
                                Paths.get(containerImagePath),
                                containerImage.imageName))
                        .orElseGet(() -> new ContainerImage(containerImage.protocol,
                                Paths.get(""),
                                containerImage.imageName));
            }
        };
    }

    static Optional<String> getLocalImagesDir(PullSingularityContainerArgs args, String defaultContainerImagesDir) {
        if (StringUtils.isNotBlank(args.containerImagesDirectory)) {
            return Optional.of(args.containerImagesDirectory);
        } else if (StringUtils.isNotBlank(defaultContainerImagesDir)) {
            return Optional.of(defaultContainerImagesDir);
        } else {
            return Optional.empty();
        }
    }

    /**
     * @param containerLocation
     * @return a pair of the container directory and the parsed out container name. Container directory is only set if
     * the container location is a local path
     */
    private static ContainerImage getLocalContainerImage(String containerLocation) {
        if (StringUtils.isBlank(containerLocation)) {
            throw new IllegalArgumentException("Container location cannot be empty");
        }
        if (StringUtils.startsWithIgnoreCase(containerLocation, "shub://")) {
            return new ContainerImage("shub",
                    null,
                    parseDockerHubOrSHubName(containerLocation.substring("shub://".length())));
        } else if (StringUtils.startsWithIgnoreCase(containerLocation, "docker://")) {
            return new ContainerImage("docker",
                    null,
                    parseDockerHubOrSHubName(containerLocation.substring("docker://".length())));
        } else if (StringUtils.startsWithIgnoreCase(containerLocation, "http://") ||
                StringUtils.startsWithIgnoreCase(containerLocation, "https://")) {
            URI containerLocationURI = URI.create(containerLocation);
            String containerURIPath;
            if (StringUtils.isNotBlank(containerLocationURI.getPath())) {
                containerURIPath = containerLocationURI.getPath();
            } else {
                containerURIPath = containerLocationURI.getHost() +
                        (containerLocationURI.getPort() > 0 ? "-" + containerLocationURI.getPort() : "");
            }
            return new ContainerImage("http",
                    null,
                    Paths.get(containerURIPath).getFileName().toString());
        } else if (StringUtils.startsWithIgnoreCase(containerLocation, "file:/")) {
            Path containerImagePath = new File(URI.create(containerLocation)).toPath();
            return new ContainerImage("file",
                    containerImagePath.getParent(),
                    containerImagePath.getFileName().toString());
        } else if (StringUtils.startsWithIgnoreCase(containerLocation, "/")) {
            // assume a path
            Path containerImagePath = Paths.get(containerLocation);
            return new ContainerImage("file",
                    containerImagePath.getParent(),
                    containerImagePath.getFileName().toString());
        } else {
            // assume a docker registry name
            return new ContainerImage("docker",
                    null,
                    parseDockerHubOrSHubName(containerLocation));

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
    private static Pair<String, String> extractComponent(String s, char separator,
                                                         Function<String, Pair<String, String>> missingComponentHandler) {
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
}
