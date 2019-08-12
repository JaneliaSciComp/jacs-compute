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
import org.janelia.model.security.util.SubjectUtils;

/**
 * Authenticated user attribute
 */
public class AuthenticatedUserAttribute implements ExchangeAttribute {

    private static final String USERNAME_ATTRIBUTE = "Username";
    private static final String RUNAS_ATTRIBUTE = "RunAsUser";
    private static final String JACS_SUBJECT_ATTRIBUTE = "JacsSubject";

    @Override
    public String readAttribute(HttpServerExchange exchange) {
        SecurityContext sc = exchange.getSecurityContext();

        String authenticatedUser;
        if (sc != null && sc.isAuthenticated()) {
            authenticatedUser = sc.getAuthenticatedAccount().getPrincipal().getName();
        }
        else {
            HeaderValues usernameHeader = exchange.getRequestHeaders().get(USERNAME_ATTRIBUTE);
            if (usernameHeader == null || usernameHeader.size() == 0) {
                HeaderValues subjectHeader = exchange.getRequestHeaders().get(JACS_SUBJECT_ATTRIBUTE);
                if (subjectHeader == null || subjectHeader.size() == 0) {
                    authenticatedUser = "unknown";
                }
                else {
                    authenticatedUser = subjectHeader.getFirst();
                }
            }
            else {
                authenticatedUser = usernameHeader.getFirst();
            }
        }

        // By default, the authenticated user is used as the authorized user
        String authorizedUser = authenticatedUser;

        HeaderValues runAsHeader = exchange.getRequestHeaders().get(RUNAS_ATTRIBUTE);
        if (runAsHeader != null && runAsHeader.size() > 0) {
            authorizedUser = runAsHeader.getFirst();
        }

        return SubjectUtils.getSubjectName(authenticatedUser) + " " + SubjectUtils.getSubjectName(authorizedUser);
    }

    @Override
    public void writeAttribute(HttpServerExchange exchange, String newValue) throws ReadOnlyAttributeException {
        throw new ReadOnlyAttributeException("Authenticated user", newValue);
    }

}
