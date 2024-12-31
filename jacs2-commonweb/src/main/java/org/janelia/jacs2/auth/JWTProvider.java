package org.janelia.jacs2.auth;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.SecretKey;
import jakarta.inject.Inject;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.MalformedJwtException;
import io.jsonwebtoken.UnsupportedJwtException;
import io.jsonwebtoken.lang.Strings;
import io.jsonwebtoken.security.Keys;
import io.jsonwebtoken.security.SignatureException;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.security.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles JWT security tokens abstractly, without exposing the details of the implementation.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class JWTProvider {

    private static final Logger LOG = LoggerFactory.getLogger(JWTProvider.class);

    public static final String TYPE_HEADER = "typ";
    public static final String USERNAME_CLAIM = "user_name";
    static final String FULLNAME_CLAIM = "full_name";
    static final String EMAIL_CLAIM = "mail";
    static final String GROUPS_CLAIM = "groups";

    private byte[] secretKeyBytes;

    @Inject
    public JWTProvider(@PropertyValue(name = "JWT.SecretKey") String secretKey) {
        if (StringUtils.isBlank(secretKey)) {
            LOG.warn("You must configure a JWT.SecretKey in your properties file");
            this.secretKeyBytes = new byte[32];
        } else {
            this.secretKeyBytes = secretKey.getBytes();
        }
    }

    public String encodeJWT(User user) {

        Object[] readGroups = user.getReadGroups().stream().map(f -> f.replace("group:", "")).toArray();
        String readGroupStr = Strings.arrayToCommaDelimitedString(readGroups);

        Date now = new Date();
        Date tomorrow = new Date(now.getTime() + 24 * 60 * 60 *1000);
        SecretKey key = Keys.hmacShaKeyFor(secretKeyBytes);
        return Jwts.builder()
                .setHeaderParam(TYPE_HEADER, "JWT") // required by nginx-jwt library
                .setExpiration(tomorrow)
                .claim(USERNAME_CLAIM, user.getName())
                .claim(FULLNAME_CLAIM, user.getFullName())
                .claim(EMAIL_CLAIM, user.getEmail())
                .claim(GROUPS_CLAIM, readGroupStr)
                .signWith(key)
                .compact();
    }

    /**
     * Returns the claims for the given JWT, if it can be verified. If not, null is returned.
     * The claims use keys given by the constants defined in this class.
     * @param jws Signed JWT
     * @return map of
     */
    public Map<String, String> decodeJWT(String jws) {
        SecretKey key = Keys.hmacShaKeyFor(secretKeyBytes);
        try {
            Claims body = Jwts.parser().setSigningKey(key).parseClaimsJws(jws).getBody();
            Map<String,String> claims = new HashMap<>();
            // Translate claim values to strings
            for (Map.Entry<String, Object> entry : body.entrySet()) {
                if (entry.getValue()!=null) {
                    claims.put(entry.getKey(), entry.getValue().toString());
                }
            }
            return claims;
        } catch (ExpiredJwtException | UnsupportedJwtException | MalformedJwtException | SignatureException e) {
            LOG.debug("Invalid JWT due to {}: {}", e.getClass().getSimpleName(), e.getMessage());
            return null;
        }
    }

    public boolean verifyJWT(String jws) {
        return decodeJWT(jws) != null;
    }
}
