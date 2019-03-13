package org.janelia.jacs2.auth;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import org.janelia.model.security.GroupRole;
import org.janelia.model.security.User;
import org.junit.Test;

import static org.junit.Assert.*;

public class JWTProviderTest {

    @Test
    public void testJWTCreation() {
        String SECRET_KEY = "tQlva5Ls8LCi7DPPsQeOZI4euHJ45ZXGOCRYqzyec37JXfKCT6a7sLMShR6KpGE";
        String userName = "testuser";
        String userFullName = "Test User";
        String userEmail = "test@test.com";
        JWTProvider jp = new JWTProvider(SECRET_KEY);
        User user = new User();
        user.setName(userName);
        user.setFullName(userFullName);
        user.setEmail(userEmail);
        String jws = jp.encodeJWT(user);
        assertNotNull(jws);
        assertTrue(jp.verifyJWT(jws));
    }

    @Test
    public void testJWTCreation2() {
        String SECRET_KEY = "dfd__dfdfdfddfdf_udqKMfdfdfdfdfYegRFEdfdfdfdfgadgA";
        String userName = "root";
        String userFullName = "Default Administrative User";
        String userEmail = "mail";
        JWTProvider jp = new JWTProvider(SECRET_KEY);
        User user = new User();
        user.setName(userName);
        user.setFullName(userFullName);
        user.setEmail(userEmail);
        user.setUserGroupRole("group:testers", GroupRole.Reader);
        String jws = jp.encodeJWT(user);
        assertNotNull(jws);
        Jws<Claims> claimsJws = jp.decodeJWT(jws);
        assertNotNull(claimsJws);
        assertEquals(userName, claimsJws.getBody().get("user_name", String.class));
        assertEquals(userFullName, claimsJws.getBody().get("full_name", String.class));
        assertEquals(userEmail, claimsJws.getBody().get("mail", String.class));
        assertEquals("testers", claimsJws.getBody().get("groups", String.class));
    }

}
