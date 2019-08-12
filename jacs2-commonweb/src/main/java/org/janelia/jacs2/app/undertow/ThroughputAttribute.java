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

import io.undertow.attribute.ExchangeAttribute;
import io.undertow.attribute.ReadOnlyAttributeException;
import io.undertow.server.HttpServerExchange;

/**
 * Throughput attribute
 */
public class ThroughputAttribute implements ExchangeAttribute {

    @Override
    public String readAttribute(final HttpServerExchange exchange) {
        long bytesSent = exchange.getResponseBytesSent();
        long requestStartTime = exchange.getRequestStartTime();
        if (bytesSent == 0) {
            return ""; // N/A
        } else {
            final long nanos;
            if (requestStartTime <= 0) {
                return ""; // N/A
            } else {
                nanos = System.nanoTime() - requestStartTime;
            }
            double tp = (bytesSent * 8  * 1000.) / nanos;
            return String.format("%.3f", tp);
        }
    }

    @Override
    public void writeAttribute(final HttpServerExchange exchange, final String newValue) throws ReadOnlyAttributeException {
        throw new ReadOnlyAttributeException("Throughput", newValue);
    }

}
