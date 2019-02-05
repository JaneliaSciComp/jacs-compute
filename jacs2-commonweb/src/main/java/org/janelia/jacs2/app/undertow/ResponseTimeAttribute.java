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
import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.TimeUnit;

/**
 * Response attribute in millis
 */
public class ResponseTimeAttribute implements ExchangeAttribute {

    @Override
    public String readAttribute(final HttpServerExchange exchange) {
        long requestStartTime = exchange.getRequestStartTime();
        if(requestStartTime == -1) {
            return "";
        } else {
            final long nanos = System.nanoTime() - requestStartTime;
            StringBuilder buf = new StringBuilder();
            long milis = TimeUnit.MILLISECONDS.convert(nanos, TimeUnit.NANOSECONDS);
            buf.append(milis / 1000);
            buf.append('.');
            int remains = (int) (milis % 1000);
            remains = remains % 100;
            buf.append(remains / 10);
            buf.append(remains % 10);
            return buf.toString();
        }
    }

    @Override
    public void writeAttribute(final HttpServerExchange exchange, final String newValue) throws ReadOnlyAttributeException {
        throw new ReadOnlyAttributeException("Response time", newValue);
    }

}
