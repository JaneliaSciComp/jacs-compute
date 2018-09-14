package org.janelia.jacs2.asyncservice.containerizedservices;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.SubParameter;
import com.beust.jcommander.converters.CommaParameterSplitter;
import com.beust.jcommander.converters.DefaultListConverter;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class RunSingularityContainerArgs extends AbstractSingularityContainerArgs {
    enum ContainerOperation {
        run,
        exec
    }

    static class BindPath {
        @SubParameter(order = 0)
        String srcPath; // path outside the container
        @SubParameter(order = 1)
        String targetPath; // path inside the container
        @SubParameter(order = 2)
        String mountOpts; // mount options

        boolean isEmpty() {
            return StringUtils.isEmpty(srcPath);
        }

        boolean isNotEmpty() {
            return StringUtils.isNotEmpty(srcPath);
        }

        String asString() {
            StringBuilder specBuilder = new StringBuilder();
            specBuilder.append(srcPath);
            if (StringUtils.isNotBlank(targetPath) || StringUtils.isNotBlank(mountOpts)) {
                specBuilder.append(':');
                specBuilder.append(targetPath);
                if (StringUtils.isNotBlank(mountOpts)) {
                    specBuilder.append(':');
                    specBuilder.append(mountOpts);
                }
            }
            return specBuilder.toString();
        }

        @Override
        public String toString() {
            return asString();
        }
    }

    static class BindPathConverter implements IStringConverter<BindPath> {
        @Override
        public BindPath convert(String value) {
            BindPath bindPath = new BindPath();
            List<String> bindArgList = Splitter.on(':').trimResults().splitToList(value);
            bindPath.srcPath = getArgAt(bindArgList, 0)
                    .orElse("");
            bindPath.targetPath = getArgAt(bindArgList, 1)
                    .orElse("");
            bindPath.mountOpts = getArgAt(bindArgList, 2)
                    .map(a -> {
                        if (a.equalsIgnoreCase("ro")) {
                            return "ro";
                        } else if (a.equalsIgnoreCase("rw")) {
                            return "rw";
                        } else {
                            return "";
                        }
                    })
                    .orElse("");
            return bindPath;
        }

        private Optional<String> getArgAt(List<String> bindSpecList, int index) {
            if (bindSpecList.size() > index && StringUtils.isNotBlank(bindSpecList.get(index))) {
                return Optional.of(bindSpecList.get(index));
            } else {
                return Optional.empty();
            }

        }
    }

    @Parameter(names = "-op", description = "Singularity container operation {run (default) | exec}")
    ContainerOperation operation = ContainerOperation.run;
    @Parameter(names = "-appName", description = "Containerized application Name")
    String appName;
    @Parameter(names = "-bindPaths", description = "Container bind path specs. The spec format is src[:dst[:options]], where options are 'ro' (read-only) or 'rw' (read/write). " +
            "If the mount options are specified and the value does not match 'ro' or 'rw' the options will default to 'rw'",
            converter = BindPathConverter.class)
    List<BindPath> bindPaths = new ArrayList<>();
    @Parameter(names = "-overlay", description = "Container overlay")
    String overlay;
    @Parameter(names = "-enableNV", description = "Enable NVidia support")
    boolean enableNV;
    @Parameter(names = "-initialPwd", description = "Initial working directory inside the container")
    String initialPwd;
    @Parameter(names = "-appArgs", description = "Containerized application arguments", splitter = ServiceArgSplitter.class)
    List<String> appArgs = new ArrayList<>();

    RunSingularityContainerArgs() {
        this("Service that runs a singularity container");
    }

    RunSingularityContainerArgs(String description) {
        super(description);
    }

    String bindPathsAsString() {
        return bindPaths.stream()
                .filter(RunSingularityContainerArgs.BindPath::isNotEmpty)
                .map(RunSingularityContainerArgs.BindPath::asString)
                .reduce((s1, s2) -> s1.trim() + "," + s2.trim())
                .orElse("");
    }
}
