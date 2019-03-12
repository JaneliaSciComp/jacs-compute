package org.janelia.jacs2.auth;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.security.User;

import javax.inject.Inject;
import java.security.SecureRandom;
import java.util.Date;

/**
 * Handling of JWT security tokens.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class JWTProvider {

    private byte[] secretKeyBytes;

    @Inject
    public JWTProvider(@PropertyValue(name = "JWT.SecretKey") String secretKey) {
        this.secretKeyBytes = secretKey.getBytes();
    }

    public String generateJWT(User user) throws JOSEException {

        Date now = new Date();
        Date tomorrow = new Date(now.getTime() + 24 * 60 * 60 *1000);

        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .subject(user.getName())
                .expirationTime(tomorrow)
                .claim("user_name", user.getName())
                .claim("full_name", user.getFullName())
                .claim("mail", user.getEmail())
                .build();
        SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims);

        // Apply the HMAC to the JWS object
        new SecureRandom().nextBytes(secretKeyBytes);
        jwt.sign(new MACSigner(secretKeyBytes));

        // Output in URL-safe format
        return jwt.serialize();
    }
}
