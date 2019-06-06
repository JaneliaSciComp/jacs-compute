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

/**
 * Name value attribute
 */
public class NameValueAttribute implements ExchangeAttribute {

    private final String name;
    private final ExchangeAttribute valueAttr;
    private final boolean ignoreIfEmpty;

    public NameValueAttribute(String name, ExchangeAttribute valueAttr) {
        this.name = name;
        this.valueAttr = valueAttr;
        this.ignoreIfEmpty = false;
    }

    public NameValueAttribute(String name, ExchangeAttribute valueAttr, boolean ignoreIfEmpty) {
        this.name = name;
        this.valueAttr = valueAttr;
        this.ignoreIfEmpty = ignoreIfEmpty;
    }

    @Override
    public String readAttribute(HttpServerExchange exchange) {
        String val = valueAttr.readAttribute(exchange);
        if (ignoreIfEmpty && StringUtils.isBlank(val)) {
            return "";
        } else {
            return name + "=" + val;
        }
    }

    @Override
    public void writeAttribute(HttpServerExchange exchange, String newValue) throws ReadOnlyAttributeException {
        valueAttr.writeAttribute(exchange, newValue);
    }

}
