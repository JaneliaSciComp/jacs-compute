/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2014 Red Hat, Inc., and individual contributors
 * as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.janelia.jacs2.app.undertow;

import java.util.function.Predicate;

import io.undertow.attribute.ExchangeAttribute;
import io.undertow.attribute.ReadOnlyAttributeException;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.HttpString;

/**
 * Request Headers attribute
 */
public class RequestHeadersAttribute implements ExchangeAttribute {

    private final Predicate<HttpString> omittedHeadersFilter;

    RequestHeadersAttribute(Predicate<HttpString> omittedHeadersFilter) {
        this.omittedHeadersFilter = omittedHeadersFilter;
    }

    @Override
    public String readAttribute(final HttpServerExchange exchange) {
        HeaderMap headers = exchange.getRequestHeaders();
        HeaderMap filteredHeaders;
        if (headers == null || omittedHeadersFilter == null) {
            filteredHeaders = headers;
        } else {
            filteredHeaders = new HeaderMap();
            headers.getHeaderNames().stream()
                    .filter(omittedHeadersFilter.negate())
                    .forEach(header -> filteredHeaders.addAll(header, headers.get(header)));
        }
        if (filteredHeaders == null || filteredHeaders.size() == 0) {
            return "{}";
        } else {
            return filteredHeaders.toString();
        }
    }

    @Override
    public void writeAttribute(final HttpServerExchange exchange, final String newValue) throws ReadOnlyAttributeException {
        throw new ReadOnlyAttributeException("Headers", newValue);
    }

}
