package org.janelia.jacs2.asyncservice.utils;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Random;

public class X11Utils {
    private static final String X_LOCK_FILE_PATTERN = "/tmp/.*X${PORT}-lock";
    private static final String X11_UNIX_SOCKET = "/tmp/.X11-unix/X${PORT}";

    public static final int START_DISPLAY_PORT = 890;
    public static final int DEFAULT_TIMEOUT_SECONDS = 3600;  // 60 minutes
    public static final int DEFAULT_RETRIES = 254;

    private static final Random PORT_GENERATOR = new Random();

    public static int getRandomPort(int startPort) {
        return startPort + PORT_GENERATOR.nextInt(256);
    }

    /**
     * @pparam outputDir
     * @param scriptWriter
     * @throws IOException
     */
    public static void setDisplayPort(String outputDir, ScriptWriter scriptWriter) throws IOException {
        scriptWriter.setVar("DISPLAY_PORT", Integer.toString(getRandomPort(START_DISPLAY_PORT)));
        startDisplayServer(outputDir, "$DISPLAY_PORT", "1280x1024x24", DEFAULT_RETRIES, scriptWriter);
    }

    public static void defineCleanXvfbFunction(ScriptWriter scriptWriter) throws IOException {
        scriptWriter
                .openFunction("cleanXvfb")
                .add("kill $MYPID")
                .addWithArgs("rm -f ")
                    .addArg(X_LOCK_FILE_PATTERN)
                    .addArg(X11_UNIX_SOCKET)
                    .endArgs("")
                .echo("Cleaned up Xvfb")
                .closeFunction("cleanXvfb");
        scriptWriter.add("trap cleanXvfb EXIT");
    }

    /**
     * Generate a script that starts the display server process based on X11 - xvfb
     * @param outputDir
     * @param displayPort
     * @param resolution
     * @param retries
     * @param scriptWriter
     * @throws IOException
     */
    public static void startDisplayServer(String outputDir, String displayPort, String resolution, int retries, ScriptWriter scriptWriter) throws IOException {
        // Skip ports that are currently in use, or "locked"
        scriptWriter
                .echo("Running on `hostname`")
                .echo(MessageFormat.format("Finding a port for Xvfb starting at {0}", displayPort))
                .setVar("PORT", displayPort)
                .setVar("COUNTER", "0")
                .setVar("RETRIES", String.valueOf(retries));

        defineCleanXvfbFunction(scriptWriter);

        scriptWriter
                .add("while [ \"$COUNTER\" -lt \"$RETRIES\" ]; do")
                .addIndent()
                .add(String.format(
                        "while (test -f \"%s\") || (netstat -atwn | grep \"^.*:${PORT}.*:\\*\\s*LISTEN\\s*$\"); do",
                        X11_UNIX_SOCKET))
                .addIndent()
                .add("PORT=$(( ${PORT} + 1 ))")
                .removeIndent()
                .add("done")
                .setVar("tryAgain", "0")
                .add(String.format("for f in \"%s\"; do", X_LOCK_FILE_PATTERN))
                .addIndent()
                .add("if [ -f $f ]; then")
                .addIndent()
                .add("PORT=$(( ${PORT} + 1 ))")
                .setVar("tryAgain", "1")
                .removeIndent()
                .add("fi")
                .removeIndent()
                .add("done")
                .add("if [ $tryAgain == 1 ]; then")
                .addIndent().add("continue").removeIndent()
                .add("fi")
                .echo("Found the first free port: $PORT")
                // Run Xvfb (virtual framebuffer) on the chosen port
                .addWithArgs("/usr/bin/Xvfb :${PORT} -screen 0 ")
                    .addArg(resolution)
                    .addArg("-fp /usr/share/X11/fonts/misc")
                    .addArg(">")
                    .addArg(Paths.get(outputDir, "Xvfb.${PORT}.log").toString())
                    .addArg("2>&1")
                    .endArgs("&")
                .echo("Started Xvfb on port $PORT")
                // Save the PID so that we can kill it when we're done
                .setVar("MYPID", "$!")
                .exportVar("DISPLAY", StringUtils.wrap("localhost:${PORT}.0", '"'))
                // Wait some time and check to make sure Xvfb is actually running, and retry if not.
                .add("sleep 3")
                .add("if kill -0 $MYPID >/dev/null 2>&1; then")
                .addIndent()
                .echo("Xvfb is running as $MYPID")
                .add("break")
                .removeIndent()
                .add("else")
                .addIndent()
                .echo("Xvfb died immediately, trying again...")
                .add("cleanXvfb")
                .setVar("PORT", "$(( ${PORT} + 1 ))")
                .removeIndent()
                .add("fi")
                .setVar("COUNTER", "$(( $COUNTER + 1 ))")
                .removeIndent()
                .add("done");
    }

    /**
     * Take a screenshot with quadratically increase latency.
     * For example, when secs=5, the time between screenshots works out to the quadratic sequence 2.5t^2+2.5t
     * which means that screenshots are taken at 5 seconds, 15 seconds, 30 seconds, 50 seconds, etc.
     * @param outputDir
     * @param portVarName
     * @param pidVarName
     * @param secs
     * @param maxSecs
     * @param scriptWriter
     */
    public static void startScreenCaptureLoop(String outputDir, String portVarName, String pidVarName, int secs, int maxSecs, ScriptWriter scriptWriter) throws IOException {
        scriptWriter
                .setVar("XVFB_SCREENSHOT_DIR", outputDir)
                .add("mkdir -p $XVFB_SCREENSHOT_DIR")
                .setVar("ssinc", String.valueOf(secs)) // increment in how often to take a screenshot
                .setVar("freq", "$ssinc") // how often to take a screenshot
                .setVar("inc","5") // how often to wake up and check if the process is still running
                .setVar("t","0") // time counter
                .setVar("nt", "$freq")
                .add("while kill -0 $" + pidVarName + " 2> /dev/null; do")
                .addIndent()
                .add("sleep $inc")
                .setVar("t","$((t+inc))")
                // Take a screenshot with an incrementing delay
                .add("if [ \"$t\" -eq \"$nt\" ]; then")
                .addIndent()
                .add("freq=$((freq+ssinc))")
                .add("nt=$((t+freq))")
                .add("DISPLAY=:$" + portVarName + " import -window root $XVFB_SCREENSHOT_DIR/screenshot_$t.png")
                .removeIndent()
                .add("fi")
                // Don't allow it to run for more than a set amount of time, in case it hangs due to requiring user input
                .add("if [ \"$t\" -gt "+maxSecs+" ]; then")
                .addIndent()
                .echo("Killing Xvfb session which has been running for over "+maxSecs+" seconds")
                .add("kill -9 $" + pidVarName + " 2> /dev/null")
                .removeIndent()
                .add("fi")
                .removeIndent()
                .add("done");
    }

}
