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

import java.util.Deque;

import io.undertow.attribute.ExchangeAttribute;
import io.undertow.attribute.ReadOnlyAttributeException;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.form.FormData;
import io.undertow.server.handlers.form.FormDataParser;
import io.undertow.util.HeaderValues;

/**
 * Response attribute in seconds.
 */
public class RequestBodyAttribute implements ExchangeAttribute {

    @Override
    public String readAttribute(final HttpServerExchange exchange) {
        FormData formData = exchange.getAttachment(FormDataParser.FORM_DATA);
        if (formData != null) {
            return getFormData(formData);
        } else if (exchange.getAttachment(SavedRequestHandler.SAVED_REQUEST_BODY) != null) {
            String body = exchange.getAttachment(SavedRequestHandler.SAVED_REQUEST_BODY).toString();
            exchange.removeAttachment(SavedRequestHandler.SAVED_REQUEST_BODY);
            return body;
        } else {
            return "";
        }
    }

    private String getFormData(FormData formData) {
        StringBuilder sb = new StringBuilder();

        sb.append("body=\n");
        for (String formField : formData) {
            Deque<FormData.FormValue> formValues = formData.get(formField);

            sb.append(formField).append("=");
            for (FormData.FormValue formValue : formValues) {
                sb.append(formValue.isFileItem() ? "[file-content]" : formValue.getValue());
                sb.append("\n");

                if (formValue.getHeaders() != null) {
                    sb.append("headers=\n");
                    for (HeaderValues header : formValue.getHeaders()) {
                        sb.append("\t")
                                .append(header.getHeaderName()).append("=").append(header.getFirst()).append("\n");
                    }
                }
            }
        }
        return sb.toString();
    }

    @Override
    public void writeAttribute(final HttpServerExchange exchange, final String newValue) throws ReadOnlyAttributeException {
        throw new ReadOnlyAttributeException("Request URL", newValue);
    }

}
