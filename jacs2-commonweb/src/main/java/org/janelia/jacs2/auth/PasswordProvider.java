package org.janelia.jacs2.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.regex.Pattern;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * Utility class for dealing with salted passwords.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@ApplicationScoped
public class PasswordProvider {
    private static final Logger LOG = LoggerFactory.getLogger(PasswordProvider.class);

    private static final int ITERATION_COUNT = 65536;
    private static final int KEY_LENGTH = 256;
    private static final int SALT_LENGTH = 16;
    private static final String DELIMITER = ".";

    /**
     * Hash the given password using PBKDF2 and return the salt concatenated with the password hash.
     * @param password plaintext password
     * @return salt + password hash
     */
    public String generatePBKDF2Hash(String password) throws NoSuchAlgorithmException, InvalidKeySpecException {
        LOG.trace("Generating salt with {} bytes...", SALT_LENGTH);
        byte[] saltBytes = getSalt();
        LOG.trace("Creating key with {} bytes...", KEY_LENGTH);
        PBEKeySpec pbeKeySpec = new PBEKeySpec(password.toCharArray(), saltBytes, ITERATION_COUNT, KEY_LENGTH);
        SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
        byte[] hashBytes = secretKeyFactory.generateSecret(pbeKeySpec).getEncoded();
        return Base64.getEncoder().encodeToString(saltBytes) + DELIMITER + Base64.getEncoder().encodeToString(hashBytes);
    }

    /**
     * Hashes the given plaintext password and verifies that it matches the hash.
     * @param plaintextPassword plaintext password to test
     * @param passwordHash stored salt + password hash
     * @return true if the passwords match
     */
    public boolean verifyPassword(String plaintextPassword, String passwordHash) throws NoSuchAlgorithmException, InvalidKeySpecException {
        String[] splitPass = passwordHash.split(Pattern.quote(DELIMITER));
        byte[] salt = Base64.getDecoder().decode(splitPass[0]);
        byte[] dHash = Base64.getDecoder().decode(splitPass[1]);
        PBEKeySpec pbeKeySpec = new PBEKeySpec(plaintextPassword.toCharArray(), salt, ITERATION_COUNT, KEY_LENGTH);
        SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
        byte[] newHash = secretKeyFactory.generateSecret(pbeKeySpec).getEncoded();
        if (newHash.length != dHash.length) {
            return false;
        }
        else {
            for (int i = 0; i < newHash.length; i++) {
                if (newHash[i] != dHash[i]) return false;
            }
        }
        return true;
    }

    private static byte[] getSalt() {
        SecureRandom random = new SecureRandom();
        byte[] salt = new byte[SALT_LENGTH];
        random.nextBytes(salt);
        return salt;
    }
}
