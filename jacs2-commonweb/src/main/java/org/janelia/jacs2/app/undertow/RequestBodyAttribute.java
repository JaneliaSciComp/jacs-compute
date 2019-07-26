package org.janelia.jacs2.app.undertow;

import java.util.List;

import io.undertow.attribute.ExchangeAttribute;
import io.undertow.attribute.ReadOnlyAttributeException;
import io.undertow.server.HttpServerExchange;

/**
 * Request body attribute. This only works together with SavedRequestBodyHandler because
 * it expects the request body to be saved as an exchange attachment.
 */
public class RequestBodyAttribute implements ExchangeAttribute {

    private final Integer maxLength;

    RequestBodyAttribute(Integer maxLength) {
        this.maxLength = maxLength;
    }

    @Override
    public String readAttribute(final HttpServerExchange exchange) {
        List<RequestBodyPart> bodyParts = exchange.removeAttachment(SavedRequestBodyHandler.SAVED_REQUEST_BODY);
        if (bodyParts == null) {
            return null;
        } else {
            return bodyParts.stream()
                    .map(rp -> rp.partBodyBuilder.toString().trim())
                    .filter(rp -> rp.length() > 0)
                    .map(rp -> rp.replace("\r\n", "<newline>"))
                    .map(rp -> rp.replace("\n", "<newline>"))
                    .reduce(new StringBuilder(),
                            (res, rb) -> {
                                if (maxLength == null || res.length() < maxLength) {
                                    res.append(rb);
                                    if (maxLength != null && res.length() >= maxLength) {
                                        res.setLength(maxLength);
                                        res.append("... truncated ...");
                                    }
                                }
                                return res;
                            },
                            (r1, r2) -> {
                                if (maxLength == null || r1.length() < maxLength) {
                                    r1.append(r2);
                                    if (maxLength != null && r1.length() >= maxLength) {
                                        r1.setLength(maxLength);
                                        r1.append("... truncated ...");
                                    }
                                }
                                return r1;

                            }).toString();
        }
    }

    @Override
    public void writeAttribute(final HttpServerExchange exchange, final String newValue) throws ReadOnlyAttributeException {
        throw new ReadOnlyAttributeException("Request URL", newValue);
    }

}
