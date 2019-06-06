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
        if (exchange.getAttachment(SavedRequestHandler.SAVED_REQUEST_BODY) != null) {
            String body = exchange.getAttachment(SavedRequestHandler.SAVED_REQUEST_BODY).toString();
            exchange.removeAttachment(SavedRequestHandler.SAVED_REQUEST_BODY);
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
