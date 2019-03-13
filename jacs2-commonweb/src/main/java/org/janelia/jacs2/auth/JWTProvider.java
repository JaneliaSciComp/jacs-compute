package org.janelia.jacs2.auth;

import io.jsonwebtoken.*;
import io.jsonwebtoken.lang.Strings;
import io.jsonwebtoken.security.Keys;
import io.jsonwebtoken.security.SignatureException;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.security.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import javax.inject.Inject;
import java.util.Date;

/**
 * Handling of JWT security tokens.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class JWTProvider {

    private static final Logger LOG = LoggerFactory.getLogger(JWTProvider.class);

    private byte[] secretKeyBytes;

    @Inject
    public JWTProvider(@PropertyValue(name = "JWT.SecretKey") String secretKey) {
        this.secretKeyBytes = secretKey.getBytes();
    }

    public String encodeJWT(User user) {

        Object[] readGroups = user.getReadGroups().stream().map(f -> f.replace("group:", "")).toArray();
        String readGroupStr = Strings.arrayToCommaDelimitedString(readGroups);

        Date now = new Date();
        Date tomorrow = new Date(now.getTime() + 24 * 60 * 60 *1000);
        SecretKey key = Keys.hmacShaKeyFor(secretKeyBytes);
        return Jwts.builder()
                .setExpiration(tomorrow)
                .claim("groups", readGroupStr)
                .claim("user_name", user.getName())
                .claim("full_name", user.getFullName())
                .claim("mail", user.getEmail())
                .signWith(key)
                .compact();
    }

    public Jws<Claims> decodeJWT(String jws) {
        SecretKey key = Keys.hmacShaKeyFor(secretKeyBytes);
        try {
            return Jwts.parser().setSigningKey(key).parseClaimsJws(jws);
        }
        catch (ExpiredJwtException | UnsupportedJwtException | MalformedJwtException | SignatureException e) {
            LOG.debug("Invalid JWT due to {}: {}"+e.getClass().getSimpleName(), e.getMessage());
            return null;
        }
    }

    public boolean verifyJWT(String jws) {
        return decodeJWT(jws) != null;
    }
}
