package org.janelia.jacs2.asyncservice.utils;

import java.io.IOException;

public class ScriptUtils {

    public static void createTempDir(String cleanupFunctionName, String tempParentDir, ScriptWriter scriptWriter) throws IOException {
        scriptWriter
                .addWithArgs("mkdir -p ").endArgs(tempParentDir)
                .setVar("TEMP_DIR", "`TMPDIR="+tempParentDir+" mktemp -d`")
                .openFunction(cleanupFunctionName)
                .add("rm -rf $TEMP_DIR")
                .echo("Cleaned up $TEMP_DIR")
                .closeFunction(cleanupFunctionName);
    }

}
