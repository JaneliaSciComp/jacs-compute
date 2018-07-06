package org.janelia.jacs2.asyncservice.dataimport;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;

import java.util.ArrayList;
import java.util.List;

class CommonDataNodeArgs extends ServiceArgs {
    @Parameter(names = {"-dataNodeName", "-folderName"}, description = "Data node name")
    String dataNodeName;
    @Parameter(names = {"-parentDataNodeId", "-parentFolderId"}, description = "Parent data node ID")
    Long parentDataNodeId;
    @Parameter(names = {"-skipMIPS", "-noMIPS"}, description = "If set do not generate MIPs for the content")
    boolean skipMIPS = false;
    @Parameter(names = {"-standaloneMIPS"}, description = "If set the MIPS are created as images - otherwise they are added to the image stack of the source")
    boolean standaloneMIPS = false;
    @Parameter(names = "-mipsExtensions", description = "list of extensions for which to generate mips")
    List<String> mipsExtensions = new ArrayList<>(ImmutableList.of(
            ".lsm", ".tif", ".raw", ".v3draw", ".vaa3draw", ".v3dpbd", ".pbd"
    ));

    CommonDataNodeArgs(String serviceDescription) {
        super(serviceDescription);
    }

    boolean generateMIPS() {
        return !skipMIPS;
    }
}
