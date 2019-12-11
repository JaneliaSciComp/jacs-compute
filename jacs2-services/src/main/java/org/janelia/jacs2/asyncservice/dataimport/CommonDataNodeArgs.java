package org.janelia.jacs2.asyncservice.dataimport;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;

class CommonDataNodeArgs extends ServiceArgs {
    @Parameter(names = {"-dataNodeName", "-folderName"}, description = "Data node name")
    String dataNodeName;
    @Parameter(names = "-mirrorFolders", description = "If used it will try to mirror source folders under the specifid folderName")
    boolean mirrorSourceFolders;
    @Parameter(names = {"-parentDataNodeId", "-parentFolderId"}, description = "Parent data node ID")
    Long parentDataNodeId;
    @Parameter(names = {"-parentWorkspaceOwnerKey"}, description = "Parent workspace owner key")
    String parentWorkspaceOwnerKey;
    @Parameter(names = {"-skipMIPS", "-noMIPS"}, description = "If set do not generate MIPs for the content")
    boolean skipMIPS = false;
    @Parameter(names = {"-standaloneMIPS"}, description = "If set the MIPS are created as images - otherwise they are added to the image stack of the source")
    boolean standaloneMIPS = false;
    @Parameter(names = "-mipsExtensions", description = "list of extensions for which to generate mips")
    List<String> mipsExtensions = new ArrayList<>(ImmutableList.of(
            ".lsm", ".tif", "tiff", ".raw", ".v3draw", ".vaa3draw", ".v3dpbd", ".pbd", ".nrrd", ".h5j"
    ));
    @Parameter(names = "-mipsChanSpec", description = "MIPS channel spec - all files must have the same channel spec")
    String mipsChanSpec = "r"; // default to a single reference channel
    @Parameter(names = "-mipsColorSpec", description = "MIPS color spec - if specified all files must have the same color spec")
    String mipsColorSpec;
    @Parameter(names = "-mipsOptions", description = "MIPS Options")
    String mipsOptions = "mips:movies";

    CommonDataNodeArgs(String serviceDescription) {
        super(serviceDescription);
    }

    boolean generateMIPS() {
        return !skipMIPS;
    }

    boolean isMipsSupported(String ext) {
        return StringUtils.isNotBlank(ext) && mipsExtensions.contains(ext.toLowerCase());
    }
}
