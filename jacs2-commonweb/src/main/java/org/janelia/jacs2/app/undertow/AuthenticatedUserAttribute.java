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

import java.util.Optional;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.undertow.attribute.ExchangeAttribute;
import io.undertow.attribute.ReadOnlyAttributeException;
import io.undertow.security.api.SecurityContext;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.auth.JWTProvider;
import org.janelia.model.security.util.SubjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authenticated user attribute
 */
public class AuthenticatedUserAttribute implements ExchangeAttribute {
    private static final Logger LOG = LoggerFactory.getLogger(AuthenticatedUserAttribute.class);

    private static final String AUTHORIZATION_ATTRIBUTE = "Authorization";
    private static final String USERNAME_ATTRIBUTE = "Username";
    private static final String RUNAS_ATTRIBUTE = "RunAsUser";
    private static final String JACS_SUBJECT_ATTRIBUTE = "JacsSubject";

    private static final String BEARER_PREFIX = "Bearer ";

    @Override
    public String readAttribute(HttpServerExchange exchange) {
        SecurityContext sc = exchange.getSecurityContext();

        String authenticatedUser;
        if (sc != null && sc.isAuthenticated()) {
            authenticatedUser = sc.getAuthenticatedAccount().getPrincipal().getName();
        }
        else {
            authenticatedUser = getHeaderValue(exchange, USERNAME_ATTRIBUTE)
                    .orElseGet(() -> getHeaderValue(exchange, JACS_SUBJECT_ATTRIBUTE)
                            .orElseGet(() -> getHeaderValue(exchange, AUTHORIZATION_ATTRIBUTE)
                                    .flatMap(authorization -> {
                                        if (StringUtils.startsWithIgnoreCase(authorization, BEARER_PREFIX)) {
                                            return Optional.of(authorization.substring(BEARER_PREFIX.length()).trim());
                                        } else {
                                            return Optional.empty();
                                        }
                                    })
                                    .filter(StringUtils::isNotBlank)
                                    .flatMap(token -> {
                                        String[] s = token.split("\\.");

                                        if (s.length < 3) {
                                            return Optional.empty();
                                        }
                                        // take only the payload and decode it
                                        String claimsPayload =  token.split("\\.")[1];
                                        Claims body;
                                        try {
                                            body = Jwts.parser().parseClaimsJwt("." + claimsPayload + ".").getBody();
                                        } catch (Exception e) {
                                            LOG.error("Error get JWT claims from {} -> {}", claimsPayload, e.getMessage());
                                            return Optional.empty();
                                        }
                                        // then try to get the username from the claims body
                                        Object usernameClaim = body.get(JWTProvider.USERNAME_CLAIM);
                                        if (usernameClaim != null) {
                                            return Optional.of((String) usernameClaim);
                                        } else {
                                            return Optional.empty();
                                        }
                                    })
                                    .orElse("unknown"))
                    )
            ;
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

    private Optional<String> getHeaderValue(HttpServerExchange exchange, String attributeName) {
        HeaderValues headerValues = exchange.getRequestHeaders().get(attributeName);
        if (headerValues != null && headerValues.size() > 0) {
            return Optional.of(headerValues.getFirst());
        } else {
            return Optional.empty();
        }
    }
}
