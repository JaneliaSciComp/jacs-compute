package org.janelia.jacs2.asyncservice.spark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.lang3.StringUtils;

public abstract class AbstractSparkApp implements SparkApp {
    private final String errorFilename;
    private long lastCheckedErrorFileSize;
    private String errorMessage;

    public AbstractSparkApp(String errorFilename) {
        this.errorFilename = errorFilename;
        this.lastCheckedErrorFileSize = 0;
    }

    @Override
    public String getErrors() {
        if (checkForErrors()) {
            return errorMessage;
        } else {
            return null;
        }
    }

    private boolean checkForErrors() {
        if (StringUtils.isNotBlank(errorFilename)) {
            File errorFile = new File(errorFilename);
            if (errorFile.exists() && errorFile.length() > lastCheckedErrorFileSize) {
                try (InputStream errorFileStream = new FileInputStream(errorFilename)) {
                    checkStreamForErrors(errorFileStream);
                    lastCheckedErrorFileSize = errorFile.length();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
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
                if (isError(l)) {
                    errorMessage = l;
                    break;
                }
            } catch (IOException e) {
                break;
            }
        }
    }

    private boolean isError(String l) {
        return l.matches("(?i:.*(error|exception|Segmentation fault|core dumped).*)");
    }

}
