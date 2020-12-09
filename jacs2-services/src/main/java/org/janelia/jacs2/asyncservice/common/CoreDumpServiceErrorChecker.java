package org.janelia.jacs2.asyncservice.common;

import org.slf4j.Logger;

public class CoreDumpServiceErrorChecker extends DefaultServiceErrorChecker {

    public CoreDumpServiceErrorChecker(Logger logger) {
        super(logger);
    }

    @Override
    protected boolean hasErrors(String l) {
        if (l.matches("(?i:.*(Segmentation fault|core dumped).*)")) {
            // core dump is still an error
            logger.error(l);
            return true;
        } else if (l.matches("(?i:.*(error|exception).*)")) {
            // ignore any exception - just log it
            logger.warn(l);
            return false;
        } else {
            return false;
        }
    }
}
