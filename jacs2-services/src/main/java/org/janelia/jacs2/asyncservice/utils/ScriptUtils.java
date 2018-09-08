package org.janelia.jacs2.asyncservice.utils;

public class ScriptUtils {

    public static void createTempDir(String cleanupFunctionName, String tempParentDir, ScriptWriter scriptWriter) {
        scriptWriter
                .addWithArgs("mkdir -p ").endArgs(tempParentDir)
                .setVar("TEMP_DIR", "`TMPDIR="+tempParentDir+" mktemp -d`")
                .openFunction(cleanupFunctionName)
                .add("rm -rf $TEMP_DIR")
                .echo("Cleaned up $TEMP_DIR")
                .closeFunction(cleanupFunctionName);
    }

}
