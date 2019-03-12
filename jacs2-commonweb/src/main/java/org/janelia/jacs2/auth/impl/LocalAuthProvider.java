package org.janelia.jacs2.auth.impl;

import org.janelia.jacs2.auth.PasswordProvider;
import org.janelia.model.access.domain.dao.SubjectDao;
import org.janelia.model.security.Subject;
import org.janelia.model.security.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authentication implementation which stores passwords in the Mongo database.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class LocalAuthProvider implements AuthProvider {
    private static final Logger LOG = LoggerFactory.getLogger(LocalAuthProvider.class);

    private SubjectDao subjectDao;
    private PasswordProvider pwProvider;

    LocalAuthProvider(SubjectDao subjectDao, PasswordProvider pwProvider) {
        this.subjectDao = subjectDao;
        this.pwProvider = pwProvider;
    }

    @Override
    public User authenticate(String username, String password) {
        try {
            Subject subject = subjectDao.findByName(username);
            if (!(subject instanceof User)) {
                LOG.info("Illegal attempt to authenticate as {}", subject.getKey());
                return null;
            }
            User user = (User) subject;

            // Authenticate user
            if (user.getPassword() == null || !pwProvider.verifyPassword(password, user.getPassword())) {
                LOG.info("Authentication denied for user {}", user.getName());
                return null;
            }

            return user;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public User createUser(String username) {
        // Local authentication implementation cannot create users based on external data
        return null;
    }
}
