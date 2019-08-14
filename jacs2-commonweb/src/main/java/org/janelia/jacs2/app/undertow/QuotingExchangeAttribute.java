package org.janelia.jacs2.app.undertow;

import io.undertow.attribute.ExchangeAttribute;
import io.undertow.attribute.ExchangeAttributeWrapper;
import io.undertow.attribute.ReadOnlyAttributeException;
import io.undertow.server.HttpServerExchange;

/**
 * Alternative implementation of io.undertow.attribute.QuotingExchangeAttribute which uses empty quotes instead of a
 * bare dash for null values. This is used to make parsing easier in ELK.
 *
 * Also lets the user pick what kinds of quotes to use, and fixes some bugs.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class QuotingExchangeAttribute implements ExchangeAttribute {

    private final ExchangeAttribute exchangeAttribute;
    private final char quoteChar;

    public QuotingExchangeAttribute(ExchangeAttribute exchangeAttribute) {
        this(exchangeAttribute, '\'');
    }

    public QuotingExchangeAttribute(ExchangeAttribute exchangeAttribute, char quoteChar) {
        this.exchangeAttribute = exchangeAttribute;
        this.quoteChar = quoteChar;
    }

    @Override
    public String readAttribute(HttpServerExchange exchange) {
        String svalue = exchangeAttribute.readAttribute(exchange);
        if (svalue == null || "-".equals(svalue) || svalue.isEmpty()) {
            return quoteChar+""+quoteChar;
        }

        /* Wrap all quotes in double quotes. */
        StringBuilder buffer = new StringBuilder(svalue.length() + 2);
        buffer.append(quoteChar);
        int i = 0;
        while (i < svalue.length()) {
            // Does the value contain a single quote? If so, we must encode it.
            int j = svalue.indexOf(quoteChar, i);
            if (j == -1) {
                buffer.append(svalue.substring(i));
                i = svalue.length();
            }
            else {
                buffer.append(svalue, i, j + 1);
                if (quoteChar=='"')
                    buffer.append('\'');
                else
                    buffer.append('"');
                i = j + 2;
            }
        }

        buffer.append(quoteChar);
        return buffer.toString();
    }

    @Override
    public void writeAttribute(HttpServerExchange exchange, String newValue) throws ReadOnlyAttributeException {
        throw new ReadOnlyAttributeException();
    }

    public static class Wrapper implements ExchangeAttributeWrapper {
        @Override
        public ExchangeAttribute wrap(final ExchangeAttribute attribute) {
            return new QuotingExchangeAttribute(attribute);
        }
    }
}