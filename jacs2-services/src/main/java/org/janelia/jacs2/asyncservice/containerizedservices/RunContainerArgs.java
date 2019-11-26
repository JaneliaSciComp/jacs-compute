package org.janelia.jacs2.asyncservice.containerizedservices;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;

class RunContainerArgs extends AbstractContainerArgs {
    @Parameter(names = "-appName", description = "Containerized application Name")
    String appName;
    @Parameter(names = "-bindPaths", description = "Container bind path specs. The spec format is src[:dst[:options]], where options are 'ro' (read-only) or 'rw' (read/write). " +
            "If the mount options are specified and the value does not match 'ro' or 'rw' the options will default to 'rw'",
            converter = BindPathConverter.class)
    List<BindPath> bindPaths = new ArrayList<>();
    @Parameter(names = "-appArgs", description = "Containerized application arguments", splitter = ServiceArgSplitter.class)
    List<String> appArgs = new ArrayList<>();
    @Parameter(names = "-batchJobArgs", description = "Containerized application arguments when running a batch", splitter = ServiceArgSplitter.class)
    List<String> batchJobArgs = new ArrayList<>();
    @ParametersDelegate
    ArgsExpandedAtRuntime argsExpandedAtRuntime = new ArgsExpandedAtRuntime();

    RunContainerArgs(String description) {
        super(description);
    }

    String bindPathsAsString(Set<BindPath> bindPathSet) {
        return bindPathSet.stream()
                .filter(BindPath::isNotEmpty)
                .map(BindPath::asString)
                .reduce((s1, s2) -> s1.trim() + "," + s2.trim())
                .orElse("");
    }

    boolean hasRuntimeExpandedArgs() {
        return argsExpandedAtRuntime.hasRuntimeExpandedArgs();
    }

    boolean getCancelIfNoExpandedArgs() {
        return argsExpandedAtRuntime.cancelIfEmptyExpansion;
    }

    List<String> getExpandedArgsAtRuntime() {
        return argsExpandedAtRuntime.expandArguments();
    }
}
