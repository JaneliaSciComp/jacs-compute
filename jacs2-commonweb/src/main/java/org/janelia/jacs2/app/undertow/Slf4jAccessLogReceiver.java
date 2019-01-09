package org.janelia.jacs2.app.undertow;

import io.undertow.server.handlers.accesslog.AccessLogReceiver;
import org.slf4j.Logger;

class Slf4jAccessLogReceiver implements AccessLogReceiver {
    private final Logger logger;

    Slf4jAccessLogReceiver(final Logger logger) {
        this.logger = logger;
    }

    @Override
    public void logMessage(String message) {
        logger.info(message);
    }
}
