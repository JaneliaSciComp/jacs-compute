package org.janelia.jacs2.app.undertow;

import java.util.Collection;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.AttachmentKey;
import io.undertow.util.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SavedRequestBodyHandler implements HttpHandler {
    static final AttachmentKey<String> SAVED_REQUEST_BODY = AttachmentKey.create(String.class);

    private static final Logger LOG = LoggerFactory.getLogger(SavedRequestBodyHandler.class);

    private final HttpHandler next;
    private final boolean enabled;
    private final Set<String> supportedMethods;
    private final Set<String> supportedMimeTypes;
    private final Set<String> restrictedPaths;

    SavedRequestBodyHandler(HttpHandler next, boolean enabled, Collection<String> restrictedPaths) {
        this.next = next;
        this.enabled = enabled;
        this.supportedMethods = ImmutableSet.of("PUT", "POST");
        this.supportedMimeTypes = ImmutableSet.of("application/json", "application/xml", "multipart/");
        this.restrictedPaths = ImmutableSet.copyOf(restrictedPaths);
    }

    @Override
    public void handleRequest(final HttpServerExchange exchange) throws Exception {
        if (enabled && supportedMethods.contains(exchange.getRequestMethod().toString()) && isRequestPathNotRestricted(exchange.getRelativePath())) {
            String mimeType = exchange.getRequestHeaders().getFirst(Headers.CONTENT_TYPE);
            if (mimeType != null && supportedMimeTypes.stream().filter(supportedMimeType -> mimeType.startsWith(supportedMimeType)).findAny().orElse(null) != null) {
                boolean isMultipart;
                String boundary;
                if (mimeType.startsWith("multipart/")) {
                    isMultipart = true;
                    boundary = Headers.extractQuotedValueFromHeader(mimeType, "boundary");
                    if (boundary == null) {
                        LOG.warn("Could not find boundary in multipart request with ContentType: {}, multipart data will not be available", mimeType);
                    }
                } else {
                    isMultipart = false;
                    boundary = null;
                }
                exchange.addRequestWrapper((factory, wrappedExchange) -> new RequestBodySaverStreamSourceConduit(
                        factory.create(),
                        isMultipart,
                        boundary,
                        requestBody -> wrappedExchange.putAttachment(SAVED_REQUEST_BODY, requestBody)));
            }
        }
        next.handleRequest(exchange);
    }

    private boolean isRequestPathNotRestricted(String relativePath) {
        return !restrictedPaths.contains(relativePath);
    }

}
