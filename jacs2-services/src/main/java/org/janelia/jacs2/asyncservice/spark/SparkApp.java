package org.janelia.jacs2.asyncservice.spark;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.launcher.SparkAppHandle;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.function.Consumer;

public class SparkApp {

    private final SparkCluster cluster;
    private final File outputFile;
    private final File errorFile;
    private volatile SparkAppHandle handle;
    private boolean checkedForErrorsFlag;
    private String errorMessage;

    SparkApp(SparkCluster cluster, File outputFile, File errorFile) {
        this.cluster = cluster;
        this.outputFile = outputFile;
        this.errorFile = errorFile;
    }

    public SparkCluster getCluster() {
        return cluster;
    }

    public void updateHandle(SparkAppHandle handle) {
        if (this.handle == null) this.handle = handle;
    }

    public String getAppId() {
        return handle != null ? handle.getAppId() : null;
    }

    public boolean isDone() {
        return handle != null && handle.getState().isFinal();
    }

    public boolean isError() {
        if (isDone() && handle.getState() == SparkAppHandle.State.FINISHED) {
            return checkForErrors();
        } else {
            if (!checkForErrors()) {
                errorMessage = "Spark application final state: " + handle.getState();
            }
            return true;
        }
    }

    String getErrorMessage() {
        return errorMessage;
    }

    public void kill() {
        if (handle != null) {
            handle.kill();
        }
    }

    private boolean checkForErrors() {
        if (!checkedForErrorsFlag) {
            if (errorFile != null) {
                try (InputStream errorFileStream = new FileInputStream(errorFile)) {
                    checkStreamForErrors(errorFileStream);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
            checkedForErrorsFlag = true;
        }
        return StringUtils.isNotBlank(errorMessage);
    }

    private void checkStreamForErrors(InputStream iStream) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(iStream));
        for (;;) {
            try {
                String l = reader.readLine();
                if (l == null) break;
                if (StringUtils.isEmpty(l)) {
                    continue;
                }
                if (hasErrors(l)) {
                    errorMessage = l;
                    break;
                }
            } catch (IOException e) {
                break;
            }
        }
    }

    private boolean hasErrors(String l) {
        return l.matches("(?i:.*(error|exception|Segmentation fault|core dumped).*)");
    }

}
