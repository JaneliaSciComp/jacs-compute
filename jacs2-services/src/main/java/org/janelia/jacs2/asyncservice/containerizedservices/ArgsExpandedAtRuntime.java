package org.janelia.jacs2.asyncservice.containerizedservices;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.beust.jcommander.Parameter;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.utils.FileUtils;

class ArgsExpandedAtRuntime {
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

    boolean hasRuntimeExpandedArgs() {
        return StringUtils.isNotBlank(expandedArgFlag);
    }

    List<String> expandArguments() {
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
