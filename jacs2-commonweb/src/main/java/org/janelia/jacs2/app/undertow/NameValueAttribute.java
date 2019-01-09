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
 * Name value attribute
 */
public class NameValueAttribute implements ExchangeAttribute {

    private final String name;
    private final ExchangeAttribute valueAttr;

    public NameValueAttribute(String name, ExchangeAttribute valueAttr) {
        this.name = name;
        this.valueAttr = valueAttr;
    }

    @Override
    public String readAttribute(HttpServerExchange exchange) {
        return name + "=" + valueAttr.readAttribute(exchange);
    }

    @Override
    public void writeAttribute(HttpServerExchange exchange, String newValue) throws ReadOnlyAttributeException {
        valueAttr.writeAttribute(exchange, newValue);
    }

}
