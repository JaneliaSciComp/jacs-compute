package org.janelia.jacs2.asyncservice.containerizedservices;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.beust.jcommander.Parameter;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.utils.FileUtils;

class RunContainerArgs extends AbstractContainerArgs {
    @Parameter(names = "-appName", description = "Containerized application Name")
    String appName;
    @Parameter(names = "-bindPaths", description = "Container bind path specs. The spec format is src[:dst[:options]], where options are 'ro' (read-only) or 'rw' (read/write). " +
            "If the mount options are specified and the value does not match 'ro' or 'rw' the options will default to 'rw'",
            converter = BindPathConverter.class)
    List<BindPath> bindPaths = new ArrayList<>();
    @Parameter(names = "-appArgs", description = "Containerized application arguments", splitter = ServiceArgSplitter.class)
    List<String> appArgs = new ArrayList<>();
    @Parameter(names = "-expandDir", description = "Name of the expanded directory")
    String expandedDir;
    @Parameter(names = "-expandDepth", description = "The depth of the expanded directory")
    int expandedDepth = 1;
    @Parameter(names = "-expandPattern", description = "Expanded pattern")
    String expandedPattern = "glob:**/*";
    @Parameter(names = "-expandedArgFlag", description = "Optional expanded argument flag")
    String expandedArgFlag;
    @Parameter(names = "-expandedArgList", description = "Already expanded argument list")
    List<String> expandedArgList = new ArrayList<>();
    @Parameter(names = "-cancelIfEmptyExpansion", description = "If set and the expanded argument list is empty do not run the service at all, " +
            "otherwise run it once with the other provided arguments")
    boolean cancelIfEmptyExpansion;
    @Parameter(names = "-batchJobArgs", description = "Containerized application arguments when running a batch", splitter = ServiceArgSplitter.class)
    List<String> batchJobArgs = new ArrayList<>();

    RunContainerArgs(String description) {
        super(description);
    }

    String bindPathsAsString(Collection<BindPath> bindPathSet) {
        return bindPathSet.stream()
                .filter(BindPath::isNotEmpty)
                .map(bindPath -> bindPath.asString(false))
                .reduce((s1, s2) -> s1.trim() + "," + s2.trim())
                .orElse("");
    }

    boolean hasRuntimeExpandedArgs() {
        return StringUtils.isNotBlank(expandedArgFlag);
    }

    boolean getCancelIfNoExpandedArgs() {
        return cancelIfEmptyExpansion;
    }

    List<String> getExpandedArgsAtRuntime() {
        Stream<String> expandedArgsStream;
        if (StringUtils.isNotBlank(expandedDir)) {
            expandedArgsStream = FileUtils.lookupFiles(Paths.get(expandedDir), expandedDepth, expandedPattern)
                    .filter(p -> Files.isRegularFile(p))
                    .map(Path::toString);
        } else {
            expandedArgsStream = Stream.of();
        }
        return Stream.concat(expandedArgsStream, expandedArgList.stream())
                .collect(Collectors.toList());
    }
}
