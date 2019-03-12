package org.janelia.jacs2.auth;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class PasswordProviderTest {

    @Test
    public void testHashing() throws Exception {
        String PLAINTEXT_PASSWORD = "password";
        PasswordProvider pp = new PasswordProvider();
        String hash = pp.generatePBKDF2Hash(PLAINTEXT_PASSWORD);
        assertTrue(pp.verifyPassword(PLAINTEXT_PASSWORD, hash));
    }

    @Test
    public void testHashingSpecialChars() throws Exception {
        String PLAINTEXT_PASSWORD = "1!@#$%^&*()';./.,dsfger";
        PasswordProvider pp = new PasswordProvider();
        String hash = pp.generatePBKDF2Hash(PLAINTEXT_PASSWORD);
        assertTrue(pp.verifyPassword(PLAINTEXT_PASSWORD, hash));
    }

}
