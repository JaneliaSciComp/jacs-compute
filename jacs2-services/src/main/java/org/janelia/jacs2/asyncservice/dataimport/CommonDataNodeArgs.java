package org.janelia.jacs2.asyncservice.dataimport;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;

import java.util.ArrayList;
import java.util.List;

class CommonDataNodeArgs extends ServiceArgs {
    @Parameter(names = "-dataNodeName", description = "Data node name")
    String dataNodeName;
    @Parameter(names = "-parentDataNodeId", description = "Parent data node ID")
    Long parentDataNodeId;
    @Parameter(names = "-generateMIPS", description = "Generate MIPs for the content to be uploaded and upload the MIPs as well")
    boolean generateMIPs = false;
    @Parameter(names = "-mipsExtensions", description = "list of extensions for which to generate mips")
    List<String> mipsExtensions = new ArrayList<>(ImmutableList.of(
            ".lsm", ".tif", ".raw", ".v3draw", ".vaa3draw", ".v3dpbd", ".pbd"
    ));

    CommonDataNodeArgs(String serviceDescription) {
        super(serviceDescription);
    }
}
