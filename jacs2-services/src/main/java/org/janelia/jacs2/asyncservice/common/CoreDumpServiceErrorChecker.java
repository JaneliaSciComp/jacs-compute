package org.janelia.jacs2.asyncservice.common;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class CoreDumpServiceErrorChecker extends DefaultServiceErrorChecker {

    public CoreDumpServiceErrorChecker(Logger logger) {
        super(logger);
    }

    @Override
    protected boolean hasErrors(String l) {
        if (StringUtils.isNotBlank(l)) {
            if (l.matches("(?i:.*(Segmentation fault|core dumped).*)")) {
                // core dump is still an error
                logger.error(l);
                return true;
            } else if (l.matches("(?i:.*(error|exception).*)")) {
                // ignore any exception for Fiji - just log it
                logger.warn(l);
                return false;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }
}
