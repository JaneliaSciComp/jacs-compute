package org.janelia.jacs2.user;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import jakarta.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.cluster.ComputeAccounting;
import org.janelia.jacs2.auth.impl.AuthProvider;
import org.janelia.jacs2.cdi.qualifier.BoolPropertyValue;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.notifservice.EmailNotificationService;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.domain.dao.SubjectDao;
import org.janelia.model.access.domain.dao.WorkspaceNodeDao;
import org.janelia.model.domain.workspace.Workspace;
import org.janelia.model.security.Group;
import org.janelia.model.security.GroupRole;
import org.janelia.model.security.User;
import org.janelia.model.security.UserGroupRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creating users requires a lot of actions. This class groups those actions together and provides a centralized
 * way to access user creation logic.
 */
public class UserManager {

    private static final Logger LOG = LoggerFactory.getLogger(UserManager.class);

    private SubjectDao subjectDao;
    private AuthProvider authProvider;
    private WorkspaceNodeDao workspaceNodeDao;
    private ComputeAccounting computeAccounting;
    private EmailNotificationService emailNotificationService;
    private String defaultWorkingDir;
    private String defaultReadGroups;
    private boolean newUserFileStoreCreation;
    private String newUserFileStoreEmailDestination;

    @Inject
    public UserManager(SubjectDao subjectDao,
                       AuthProvider authProvider,
                       @AsyncIndex WorkspaceNodeDao workspaceNodeDao,
                       ComputeAccounting computeAccounting,
                       EmailNotificationService emailNotificationService,
                       @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                       @PropertyValue(name = "user.defaultReadGroups") String defaultReadGroups,
                       @BoolPropertyValue(name = "user.newUser.fileStoreCreation") boolean newUserFileStoreCreation,
                       @PropertyValue(name = "user.newUser.emailNotification.destination") String newUserFileStoreEmailDestination) {
        this.subjectDao = subjectDao;
        this.authProvider = authProvider;
        this.workspaceNodeDao = workspaceNodeDao;
        this.computeAccounting = computeAccounting;
        this.emailNotificationService = emailNotificationService;
        this.defaultWorkingDir = defaultWorkingDir;
        this.defaultReadGroups = defaultReadGroups;
        this.newUserFileStoreCreation = newUserFileStoreCreation;
        this.newUserFileStoreEmailDestination = newUserFileStoreEmailDestination;
    }

    /**
     * This is the only valid way to create a new user. It's the only place with all the business
     * logic necessary once a user record exists.
     *
     * At minimum, the username must be set on the parameter object. If other attributes are set,
     * they will override whatever defaults are provided by the auth source.
     *
     * @param userMetadata user object with some metadata as described above
     * @return persisted user object
     */
    public User createUser(User userMetadata) {

        String username = userMetadata.getName();
        if (StringUtils.isBlank(username)) {
            throw new IllegalArgumentException("Username cannot be blank");
        }

        User existingUser = subjectDao.findUserByNameOrKey(username);
        if (existingUser != null) {
            throw new RuntimeException("User already exists: "+username);
        }

        User user = authProvider.generateUserInfo(username);
        if (user == null) {
            throw new RuntimeException("User could not be created: "+username);
        }

        LOG.info("Creating user {}", username);

        if (userMetadata.getEmail()!=null) {
            LOG.trace("Overriding email ({}) with {}", user.getEmail(), userMetadata.getEmail());
            user.setEmail(userMetadata.getEmail());
        }

        if (userMetadata.getFullName()!=null) {
            LOG.trace("Overriding full name ({}) with {}", user.getFullName(), userMetadata.getFullName());
            user.setFullName(userMetadata.getFullName());
        }

        if (userMetadata.getUserGroupRoles()!=null) {
            LOG.trace("Overriding group roles ({}) with {} roles", user.getUserGroupRoles().size(), userMetadata.getUserGroupRoles().size());
            user.setUserGroupRoles(userMetadata.getUserGroupRoles());
        }

        if (userMetadata.getPassword()!=null) {
            LOG.trace("Overriding password hash");
            user.setPassword(userMetadata.getPassword());
        }

        subjectDao.save(user);

        Workspace defaultWorkspace = null;

        try {

            // Create default workspace
            defaultWorkspace = workspaceNodeDao.createDefaultWorkspace(user.getKey());

            if (CollectionUtils.isEmpty(user.getUserGroupRoles())) {
                // Add to default groups
                Set<UserGroupRole> defaultUserGroupRoles = Stream.of(defaultReadGroups.split(","))
                        .map(groupKey -> new UserGroupRole(groupKey, GroupRole.Reader))
                        .collect(Collectors.toSet());
                subjectDao.updateUserGroupRoles(user, defaultUserGroupRoles);
                LOG.info("Added {} to default groups: {}", username, defaultUserGroupRoles);
                user.setUserGroupRoles(defaultUserGroupRoles);
            }

            if (newUserFileStoreCreation) {
                // Create filestore paths
                try {
                    String groupName = computeAccounting.getComputeGroup(user.getKey());
                    String fileNodeStorePath = defaultWorkingDir;
                    Path filestoreDir = Paths.get(fileNodeStorePath);

                    if (StringUtils.isBlank(groupName)) {
                        Path userDir = filestoreDir.resolve(username);
                        Files.createDirectories(userDir);
                        LOG.info("Could not determine main group for user {}}. User directory was created at {}", user.getKey(), userDir);
                    }
                    else {
                        Path groupsDir = filestoreDir.resolve("groups");
                        Path groupDir = groupsDir.resolve(groupName);
                        String groupPath = groupDir.toString();

                        if (!Files.exists(groupDir)) {
                            Files.createDirectories(groupDir);
                            LOG.info("Group directory for {} was created at {}", groupName, groupPath);
                            emailNotificationService.sendNotification(
                                    "SciComp Systems - New storage quota needed on NRS",
                                    String.format("This is an automated email generated by JACS to let SciComp Systems know "
                                            + "that a new filestore location for group %s was created at %s", groupName, groupPath),
                                    Collections.singletonList(newUserFileStoreEmailDestination)
                            );
                        }

                        Path userDir = groupDir.resolve(username);
                        Path userSymlink = filestoreDir.resolve(username);
                        Files.createDirectories(userDir);
                        Files.createSymbolicLink(userSymlink, userDir);
                        LOG.info("User directory for {} was created at {}", user.getKey(), userDir);
                    }
                }
                catch (IOException e) {
                    LOG.error("Error creating filestore directory for {}", username, e);
                }
            }

        }
        catch (Exception e) {
            LOG.error("Error configuring user. Rolling back user creation for {}", username, e);
            try {
                if (defaultWorkspace != null) {
                    workspaceNodeDao.delete(defaultWorkspace);
                }
                subjectDao.delete(user);
            }
            catch (Exception ex) {
                LOG.info("Error rolling back user creation", ex);
            }
            return null;
        }

        return user;
    }


    public Group createGroup(Group groupMetadata) {

        String groupName = groupMetadata.getName();

        Group group = subjectDao.createGroup(
                groupName,
                groupMetadata.getFullName(),
                groupMetadata.getLdapGroupName());

        try {
            workspaceNodeDao.createDefaultWorkspace(group.getKey());
        }
        catch (Exception e) {
            LOG.error("Error configuring group: {}", groupName, e);
            return null;
        }

        return group;
    }

}
