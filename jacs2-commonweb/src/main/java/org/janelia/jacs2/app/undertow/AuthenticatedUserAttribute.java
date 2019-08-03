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
import io.undertow.security.api.SecurityContext;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;
import org.apache.commons.lang3.StringUtils;

/**
 * Authenticated user attribute
 */
public class AuthenticatedUserAttribute implements ExchangeAttribute {

    private static final String USERNAME_ATTRIBUTE = "Username";
    private static final String RUNAS_ATTRIBUTE = "RunAsUser";

    @Override
    public String readAttribute(HttpServerExchange exchange) {
        String authenticatedUser;
        String authorizedUser;
        SecurityContext sc = exchange.getSecurityContext();
        if (sc != null && sc.isAuthenticated()) {
            authenticatedUser = sc.getAuthenticatedAccount().getPrincipal().getName();
        } else {
            HeaderValues usernameHeader = exchange.getRequestHeaders().get(USERNAME_ATTRIBUTE);
            if (usernameHeader == null || usernameHeader.size() == 0) {
                authenticatedUser = "";
            } else {
                authenticatedUser = usernameHeader.getFirst();
            }
        }
        HeaderValues runAsHeader = exchange.getRequestHeaders().get(RUNAS_ATTRIBUTE);
        if (runAsHeader == null || runAsHeader.size() == 0) {
            authorizedUser = "";
        } else {
            authorizedUser = runAsHeader.getFirst();
        }
        if (StringUtils.isBlank(authenticatedUser) && StringUtils.isBlank(authorizedUser)) {
            return null;
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(authenticatedUser);
            if (StringUtils.isNotBlank(authorizedUser)) {
                sb.append("/").append(authorizedUser);
            }
            return sb.toString();
        }
    }

    @Override
    public void writeAttribute(HttpServerExchange exchange, String newValue) throws ReadOnlyAttributeException {
        throw new ReadOnlyAttributeException("Authenticated user", newValue);
    }

}
