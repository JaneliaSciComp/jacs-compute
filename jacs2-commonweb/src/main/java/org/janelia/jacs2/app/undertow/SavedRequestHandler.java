package org.janelia.jacs2.app.undertow;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import io.undertow.server.ConduitWrapper;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.AttachmentKey;
import io.undertow.util.ConduitFactory;
import io.undertow.util.Headers;
import org.xnio.conduits.AbstractStreamSourceConduit;
import org.xnio.conduits.StreamSourceConduit;

public class SavedRequestHandler implements HttpHandler {

    static final AttachmentKey<StringBuilder> SAVED_REQUEST_BODY = AttachmentKey.create(StringBuilder.class);

    private static Set<String> SUPPORTED_METHODS = ImmutableSet.of("PUT", "POST");
    private static Set<String> SUPPORTED_MIMETYPPES = ImmutableSet.of(
            "application/json", "application/xml", "multipart/form-data", "multipart/mixed"
    );

    private final HttpHandler next;

    SavedRequestHandler(HttpHandler next) {
        this.next = next;
    }

    @Override
    public void handleRequest(final HttpServerExchange exchange) throws Exception {
        if (SUPPORTED_METHODS.contains(exchange.getRequestMethod().toString())) {
            String mimeType = exchange.getRequestHeaders().getFirst(Headers.CONTENT_TYPE);
            if (mimeType != null && SUPPORTED_MIMETYPPES.stream().filter(supportedMimeType -> mimeType.startsWith(supportedMimeType)).findAny().orElse(null) != null) {
                exchange.addRequestWrapper(new ConduitWrapper<StreamSourceConduit>() {
                    @Override
                    public StreamSourceConduit wrap(ConduitFactory<StreamSourceConduit> factory, HttpServerExchange exchange) {
                        return new AbstractStreamSourceConduit<StreamSourceConduit>(factory.create()) {
                            StringBuilder sb = new StringBuilder();
                            @Override
                            public int read(ByteBuffer dst) throws IOException {
                                int n = super.read(dst);
                                if (n > 0) {
                                    dst.flip();
                                    byte[] buffer = new byte[n];
                                    dst.get(buffer);
                                    sb.append(new String(buffer));
                                    if (exchange.getAttachment(SAVED_REQUEST_BODY) == null) {
                                        exchange.putAttachment(SAVED_REQUEST_BODY, sb);
                                    }
                                }
                                return n;
                            }
                        };
                    }
                });
            }

        }
        next.handleRequest(exchange);
    }

}
