package org.janelia.jacs2.asyncservice.containerizedservices;

import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

class ExpandedRunSingularityContainerArgs extends RunSingularityContainerArgs {
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

    ExpandedRunSingularityContainerArgs() {
        super("Service that runs a singularity container for each expanded argument");
    }

    boolean hasExpandedArgFlag() {
        return StringUtils.isNotBlank(expandedArgFlag);
    }
}
