package org.janelia.jacs2.user;

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

import javax.inject.Inject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Creating users requires a lot of actions. This class groups those actions together and provides a centralized
 * way to access user creation logic.
 */
public class UserManager {

    private static final Logger log = LoggerFactory.getLogger(UserManager.class);

    private SubjectDao subjectDao;
    private AuthProvider authProvider;
    private WorkspaceNodeDao workspaceNodeDao;
    private ComputeAccounting computeAccounting;
    private EmailNotificationService emailNotificationService;
    private WorkstationMailingList workstationMailingList;
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
                       WorkstationMailingList workstationMailingList,
                       @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                       @PropertyValue(name = "user.defaultReadGroups") String defaultReadGroups,
                       @BoolPropertyValue(name = "user.newUser.fileStoreCreation") boolean newUserFileStoreCreation,
                       @PropertyValue(name = "user.newUser.emailNotification.destination") String newUserFileStoreEmailDestination) {
        this.subjectDao = subjectDao;
        this.authProvider = authProvider;
        this.workspaceNodeDao = workspaceNodeDao;
        this.computeAccounting = computeAccounting;
        this.emailNotificationService = emailNotificationService;
        this.workstationMailingList = workstationMailingList;
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

        User existingUser = (User)subjectDao.findByNameOrKey(username);
        if (existingUser!=null) {
            throw new RuntimeException("User already exists: "+username);
        }

        User user = authProvider.generateUserInfo(username);
        if (user == null) {
            throw new RuntimeException("User could not be created: "+username);
        }

        log.info("Creating user {}", username);

        if (userMetadata.getEmail()!=null) {
            log.trace("Overriding email ({}) with {}", user.getEmail(), userMetadata.getEmail());
            user.setEmail(userMetadata.getEmail());
        }

        if (userMetadata.getFullName()!=null) {
            log.trace("Overriding full name ({}) with {}", user.getFullName(), userMetadata.getFullName());
            user.setFullName(userMetadata.getFullName());
        }

        if (userMetadata.getUserGroupRoles()!=null) {
            log.trace("Overriding group roles ({}) with {}", user.getUserGroupRoles().size(), userMetadata.getUserGroupRoles().size());
            user.setUserGroupRoles(userMetadata.getUserGroupRoles());
        }

        if (userMetadata.getPassword()!=null) {
            log.trace("Overriding password hash");
            user.setPassword(userMetadata.getPassword());
        }

        subjectDao.save(user);

        Workspace defaultWorkspace = null;

        try {

            // Create default workspace
            defaultWorkspace = workspaceNodeDao.createDefaultWorkspace(user.getKey());

            // Add to default groups
            Set<UserGroupRole> defaultRoles = Stream.of(defaultReadGroups.split(","))
                    .map(groupKey -> new UserGroupRole(groupKey, GroupRole.Reader))
                    .collect(Collectors.toSet());
            subjectDao.updateUserGroupRoles(user, defaultRoles);
            log.info("Added {} to default groups: {}", username, defaultReadGroups);

            if (newUserFileStoreCreation) {
                // Create filestore paths
                try {
                    String groupName = computeAccounting.getComputeGroup(user.getKey());
                    String fileNodeStorePath = defaultWorkingDir;
                    Path filestoreDir = Paths.get(fileNodeStorePath);

                    if (StringUtils.isBlank(groupName)) {
                        Path userDir = filestoreDir.resolve(username);
                        Files.createDirectories(userDir);
                        log.info("Could not determine main group for user {}}. User directory was created at {}", user.getKey(), userDir);
                    }
                    else {
                        Path groupsDir = filestoreDir.resolve("groups");
                        Path groupDir = groupsDir.resolve(groupName);
                        String groupPath = groupDir.toString();

                        if (!Files.exists(groupDir)) {
                            Files.createDirectories(groupDir);
                            log.info("Group directory for {} was created at {}", groupName, groupPath);
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
                        log.info("User directory for {} was created at {}", user.getKey(), userDir);
                    }
                }
                catch (IOException e) {
                    log.error("Error creating filestore directory for {}", username, e);
                }
            }

            if (StringUtils.isBlank(user.getEmail())) {
                log.warn("User has no email defined in LDAP, so they cannot be subscribed to the mailing list: {}", user.getKey());
            }
            else {
                try {
                    workstationMailingList.subscribe(user.getEmail(), user.getFullName());
                    log.info("Automatically subscribed user to the mailing list: {}", user.getKey());
                }
                catch (Exception e) {
                    log.warn("Error subscribing user to mailing list: {}", user.getKey(), e);
                }
            }

        }
        catch (Exception e) {
            log.error("Error configuring user. Rolling back user creation for {}", username, e);
            try {
                if (defaultWorkspace != null) {
                    workspaceNodeDao.delete(defaultWorkspace);
                }
                subjectDao.delete(user);
            }
            catch (Exception ex) {
                log.info("Error rolling back user creation", ex);
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
            log.error("Error configuring group", groupName, e);
            return null;
        }

        return group;
    }

}
