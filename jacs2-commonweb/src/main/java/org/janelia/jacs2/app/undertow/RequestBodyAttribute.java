package org.janelia.jacs2.app.undertow;

import io.undertow.attribute.ExchangeAttribute;
import io.undertow.attribute.ReadOnlyAttributeException;
import io.undertow.server.HttpServerExchange;

/**
 * Response attribute in seconds.
 */
public class RequestBodyAttribute implements ExchangeAttribute {

    @Override
    public String readAttribute(final HttpServerExchange exchange) {
        String body = exchange.getAttachment(SavedRequestBodyHandler.SAVED_REQUEST_BODY);
        if (body != null) {
            exchange.removeAttachment(SavedRequestBodyHandler.SAVED_REQUEST_BODY);
            return body;
        } else {
            return "";
        }
    }

    @Override
    public void writeAttribute(final HttpServerExchange exchange, final String newValue) throws ReadOnlyAttributeException {
        throw new ReadOnlyAttributeException("Request URL", newValue);
    }

}
