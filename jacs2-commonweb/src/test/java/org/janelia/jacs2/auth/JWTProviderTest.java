package org.janelia.jacs2.auth;

import org.janelia.model.security.User;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;

public class JWTProviderTest {

    @Test
    public void testHashing() throws Exception {
        String SECRET_KEY = "dgteig894jFIORGJGdkfgjd";
        String userName = "testuser";
        String userFullName = "Test User";
        String userEmail = "test@test.com";
        JWTProvider jp = new JWTProvider(SECRET_KEY);
        User user = new User();
        user.setName(userName);
        user.setFullName(userFullName);
        user.setEmail(userEmail);
        String jwt = jp.generateJWT(user);
        assertNotNull(jwt);
        // TODO: decode and verify JWT
    }

}
