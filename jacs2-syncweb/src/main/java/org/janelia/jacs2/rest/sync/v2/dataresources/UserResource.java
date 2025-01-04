package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.util.List;
import java.util.Map;
import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.GenericEntity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.auth.JacsSecurityContextHelper;
import org.janelia.jacs2.auth.PasswordProvider;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.jacs2.user.UserManager;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.DatasetDao;
import org.janelia.model.access.domain.dao.SubjectDao;
import org.janelia.model.domain.Preference;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.security.Group;
import org.janelia.model.security.Subject;
import org.janelia.model.security.User;
import org.janelia.model.security.UserGroupRole;
import org.janelia.model.security.dto.AuthenticationRequest;
import org.janelia.model.security.util.SubjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(name = "User", description = "Janelia Workstation User Management")
@RequireAuthentication
@Path("/data")
public class UserResource {
    private static final Logger LOG = LoggerFactory.getLogger(UserResource.class);

    @Inject
    private LegacyDomainDao legacyDomainDao;
    @Inject
    private DatasetDao datasetDao;
    @Inject
    private SubjectDao subjectDao;
    @Inject
    private PasswordProvider pwProvider;
    @Inject
    private UserManager userManager;

    @Operation(summary = "Gets a user by their subject key")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully attempted user retrieval"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error trying to get or create user")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/user")
    public Response getUser(@QueryParam("subjectKey") String subjectKey) {
        LOG.trace("Start getUser({})", subjectKey);
        if (StringUtils.isBlank(subjectKey)) {
            LOG.error("Invalid subject key ({}) provided to getUser", subjectKey);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse("Invalid subject key " + subjectKey))
                    .build();
        }
        try {
            User existingUser = subjectDao.findUserByNameOrKey(subjectKey);
            if (existingUser == null) {
                return Response.status(Response.Status.NOT_FOUND).build();
            } else {
                return Response.ok(existingUser).build();
            }
        } catch (Exception e) {
            LOG.error("Error trying to get user {}", subjectKey, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error getting user " + subjectKey))
                    .build();
        } finally {
            LOG.trace("Finished getOrCreateUser({})", subjectKey);
        }
    }

    @Operation(summary = "Creates a User",
            description = "The users's name must be populated."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully created new user"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error trying to create new user")
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/user")
    public Response createUser(@Parameter User user) {
        String userName = user.getName();
        LOG.trace("Start createUser({})", user.getName());
        try {
            String inputPassword = user.getPassword();
            if (inputPassword != null) {
                // set the password hash
                user.setPassword(pwProvider.generatePBKDF2Hash(inputPassword));
            }
            User newUser = userManager.createUser(user);
            return Response.ok(newUser).build();
        } catch (Exception e) {
            LOG.error("Error trying to create new user {}", userName, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error trying to create new user " + userName))
                    .build();
        } finally {
            LOG.trace("Finished createUser({})", userName);
        }
    }

    @Operation(summary = "Changes a user's password")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully set user password"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error setting user preferences")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/user/password")
    public User changePassword(AuthenticationRequest authenticationMessage, @Context ContainerRequestContext containerRequestContext) {
        LOG.info("changePassword({})", authenticationMessage.getUsername());
        LOG.info("changePassword({})", authenticationMessage.getPassword());
        try {
            boolean isAllowed = checkAdministrationPrivileges(authenticationMessage.getUsername(), containerRequestContext);
            if (!isAllowed) {
                throw new WebApplicationException(Response.Status.FORBIDDEN);
            }

            User user = subjectDao.findUserByNameOrKey(authenticationMessage.getUsername());
            if (user == null) {
                throw new WebApplicationException(Response.Status.NOT_FOUND);
            }
            subjectDao.setUserPassword(user, pwProvider.generatePBKDF2Hash(authenticationMessage.getPassword()));
            return user;
        } catch (Exception e) {
            LOG.error("Error occurred changing password for user {}", authenticationMessage.getUsername(), e);
            if (e instanceof WebApplicationException) {
                throw (WebApplicationException) e;
            } else {
                throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
            }
        }
    }

    /**
     * @deprecated use getUser/createUser instead of this.
     */
    @Operation(summary = "Gets a user, creating the user if necessary",
            description = "The authenticated user must be the same as the user being retrieved"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully attempted user retrieval"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error trying to get or create user")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/user/getorcreate")
    @Deprecated
    public Response getOrCreateUser(@QueryParam("subjectKey") String subjectKey) {
        LOG.trace("Start getOrCreateUser({})", subjectKey);
        if (StringUtils.isBlank(subjectKey)) {
            LOG.error("Invalid subject key ({}) provided to getOrCreateUser", subjectKey);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse("Invalid subject key " + subjectKey))
                    .build();
        }
        try {
            String username = SubjectUtils.getSubjectName(subjectKey);
            Subject existingUser = subjectDao.findUserByNameOrKey(subjectKey);
            if (existingUser == null) {
                User user = new User();
                user.setName(username);
                user = userManager.createUser(user);
                if (user != null) {
                    return Response.ok(user).build();
                }
                return Response.status(Response.Status.NOT_FOUND).build();
            } else {
                return Response.ok(existingUser).build();
            }
        } catch (Exception e) {
            LOG.error("Error trying to get or create user for {}", subjectKey, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error getting or creating user for " + subjectKey))
                    .build();
        } finally {
            LOG.trace("Finished getOrCreateUser({})", subjectKey);
        }
    }

    @Operation(summary = "Updates a user details",
            description = "The user id is used to search for the appropriate user"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated user"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error trying to update user")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/user/property")
    public Response updateUser(@Parameter Map<String, Object> userProperties, @Context ContainerRequestContext containerRequestContext) {
        // TODO: this method should update the user based on a user id, which is a stable identifier
        String username = (String) userProperties.get("name");
        try {
            LOG.info("Start updateUserProperty()");
            if (StringUtils.isBlank(username)) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorResponse("Invalid subject key " + userProperties.get("name")))
                        .build();
            }

            boolean isAllowed = checkAdministrationPrivileges(username, containerRequestContext);
            if (!isAllowed) {
                throw new WebApplicationException(Response.Status.FORBIDDEN);
            }

            User dbUser = subjectDao.findUserByNameOrKey(username);
            if (dbUser == null) {
                return Response.status(Response.Status.NOT_FOUND).build();
            } else {
                // existing user
                boolean emailR = subjectDao.updateUserProperty(dbUser, "email", (String) userProperties.get("email"));
                boolean fullNameR = subjectDao.updateUserProperty(dbUser, "fullName", (String) userProperties.get("fullname"));
                boolean nameR = subjectDao.updateUserProperty(dbUser, "name", username);
                if (emailR && fullNameR && nameR) {
                    dbUser = subjectDao.findUserByNameOrKey(username);
                    return Response.ok(dbUser).build();
                } else {
                    return Response.status(Response.Status.BAD_REQUEST).build();
                }
            }

        } catch (Exception e) {
            LOG.error("Error trying to update user {}", username, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error trying to update user roles " + username))
                    .build();
        } finally {
            LOG.trace("Finished updateUserProperty");
        }
    }


    @Operation(summary = "Updates the user's group roles",
            description = "The user id is used to search for the appropriate user"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated user property"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error trying to update user")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/user/roles")
    public Response updateUserRoles(@Parameter Set<UserGroupRole> roles,
                                    @Parameter @QueryParam("userKey") String userKey) {
        User user = subjectDao.findUserByNameOrKey(userKey);
        LOG.info("Start UpdateUser({}, {})", userKey, roles);
        if (user == null) {
            LOG.error("Invalid user ({}) provided to updateUser", userKey);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse("Invalid user " + userKey))
                    .build();
        }
        try {
            if (subjectDao.updateUserGroupRoles(user, roles)) {
                return Response.status(Response.Status.OK).build();
            } else {
                LOG.error("Could not update group roles for user {}", user);
                return Response.status(Response.Status.BAD_REQUEST).build();
            }
        } catch (Exception e) {
            LOG.error("Error trying to update user for {}", userKey, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error trying to update user roles " + userKey))
                    .build();
        } finally {
            LOG.trace("Finished updateUserRoles({})", userKey);
        }
    }

    @Operation(summary = "Creates a Group",
            description = "The group's name and fullName attributes must be populated."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully created new group"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error trying to create new group")
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/group")
    public Response createGroup(@Parameter Group group) {
        String groupName = group.getName();
        LOG.trace("Start CreateGroup({})", group.getName());
        try {
            Group newGroup = userManager.createGroup(group);
            return Response.ok(newGroup).build();
        } catch (Exception e) {
            LOG.error("Error trying to create new group {}", groupName, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error trying to create new group " + groupName))
                    .build();
        } finally {
            LOG.trace("Finished createGroup({})", groupName);
        }
    }

    @Operation(summary = "Get a List of the Workstation Users")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully got list of workstation users"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error getting list of workstation users")
    })
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/user/subjects")
    public Response getSubjects(@QueryParam("offset") Long offsetParam, @QueryParam("length") Integer lengthParam) {
        LOG.trace("Start getSubjects()");
        try {
            long offset = offsetParam != null ? offsetParam : 0;
            int length = lengthParam != null ? lengthParam : -1;
            List<Subject> subjects = subjectDao.findAll(offset, length);
            return Response
                    .ok(new GenericEntity<List<Subject>>(subjects) {
                    })
                    .build();
        } finally {
            LOG.trace("Finished getSubjects()");
        }
    }

    @Operation(summary = "Get a subject by their name or subject key")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully got subject"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error getting subject")
    })
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/user/subjects/{id}")
    public Response getSubjectById(@PathParam("id") Long subjectId) {
        LOG.trace("Start getSubjectById({})", subjectId);
        try {
            Subject s = subjectDao.findById(subjectId);
            if (s == null) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No subject found for " + subjectId))
                        .build();
            } else {
                return Response.ok(s)
                        .build();
            }
        } finally {
            LOG.trace("Finished getSubjectById({})", subjectId);
        }
    }

    @Operation(summary = "Get a subject by their name or subject key")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully got subject"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error getting subject")
    })
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/user/subject")
    public Response getSubjectByNameOrKey(@QueryParam("subjectKey") String subjectNameOrKey) {
        LOG.trace("Start getSubjectByNameOrKey({})", subjectNameOrKey);
        try {
            Subject s = subjectDao.findSubjectByNameOrKey(subjectNameOrKey);
            return Response.ok(s)
                    .build();
        } finally {
            LOG.trace("Finished getSubjectByNameOrKey({})", subjectNameOrKey);
        }
    }

    @Operation(summary = "Get a List of the User's Preferences")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully got user preferences"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error getting user preferences")
    })
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/user/preferences")
    public Response getPreferences(@Parameter @QueryParam("subjectKey") String subjectKey) {
        LOG.trace("Start getPreferences({})", subjectKey);
        try {
            List<Preference> subjectPreferences = legacyDomainDao.getPreferences(subjectKey);
            return Response
                    .ok(new GenericEntity<List<Preference>>(subjectPreferences) {
                    })
                    .build();
        } finally {
            LOG.trace("Finished getPreferences({})", subjectKey);
        }
    }

    @Operation(summary = "Sets User Preferences",
            description = "uses the Preferences Parameter of the DomainQuery."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully set user preferences"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error setting user preferences")
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/user/preferences")
    public Preference setPreferences(DomainQuery query) {
        LOG.trace("Start setPreferences({})", query);
        try {
            return legacyDomainDao.save(query.getSubjectKey(), query.getPreference());
        } catch (Exception e) {
            LOG.error("Error occurred setting preferences with {}", query, e);
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            LOG.trace("Start setPreferences({})", query);
        }
    }

    @Operation(summary = "Sets the permissions for a subject's access to a given domain object",
            description = "uses a map (targetClass=domainObject class, granteeKey = user subject key, " +
                    "rights = read, write, targetId=Id of the domainObject"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully set user preferences"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error getting user preferences")
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/user/permissions")
    public void setPermissions(@Parameter Map<String, Object> params) {
        LOG.trace("Start setPermissions({})", params);
        try {
            String subjectKey = (String) params.get("subjectKey");
            String targetClass = (String) params.get("targetClass");
            Long targetId = (Long) params.get("targetId");
            String granteeKey = (String) params.get("granteeKey");
            String rights = (String) params.get("rights");

            boolean read = rights.contains("r");
            boolean write = rights.contains("w");

            legacyDomainDao.setPermissions(subjectKey, targetClass, targetId, granteeKey, read, write, true);
        } catch (Exception e) {
            LOG.error("Error occurred setting permissions: {}", params, e);
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            LOG.trace("Finished setPermissions({})", params);
        }
    }

    @Operation(summary = "Gets a List of Members",
            description = "Uses the group key to retrieve a list of members"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully fetched the list of members"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error fetching the members")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/group/{groupKey:.*}/members")
    public List<Subject> getMembers(@Parameter @PathParam("groupKey") final String groupKey) {
        LOG.trace("Start getMembers({})", groupKey);
        try {
            return subjectDao.getGroupMembers(groupKey);
        } finally {
            LOG.trace("Finished getMembers({})", groupKey);
        }
    }

    @Operation(summary = "Get a list of groups and number of users in each group")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully got list of groups"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error getting list of groups")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/groups")
    public Map<Subject, Number> getGroups() {
        LOG.trace("Start getGroups()");
        try {
            return subjectDao.getGroupMembersCount();
        } catch (Exception e) {
            LOG.error("Error occurred getting groups", e);
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            LOG.trace("Finished getGroups()");
        }
    }

    /**
     * TODO: move this to DatasetResource
     */
    @Operation(summary = "Get a list of data sets a given group has access to read")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully got list of datasets"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error getting list of datasets")
    })
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/group/{groupName:.*}/data_sets")
    public Map<String, String> getDataSets(@Parameter @PathParam("groupName") final String groupName) {
        LOG.trace("Start getDataSets({})", groupName);
        try {
            return datasetDao.getDatasetsByGroupName(groupName);
        } catch (Exception e) {
            LOG.error("Error occurred getting group {}", groupName, e);
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            LOG.trace("Finished getDataSets({})", groupName);
        }
    }

    private boolean checkAdministrationPrivileges(String username, ContainerRequestContext containerRequestContext) {
        Subject authorizedSubject = JacsSecurityContextHelper.getAuthorizedSubject(containerRequestContext, Subject.class);
        Subject authenticatedSubject = JacsSecurityContextHelper.getAuthenticatedSubject(containerRequestContext, Subject.class);

        if (authorizedSubject == null || authenticatedSubject == null) {
            LOG.info("Unauthorized attempt to change password for {}", username);
            return false;
        }

        LOG.info("User {} is performing admin on user account for {}", authenticatedSubject.getName(), username);
        if (!authorizedSubject.getName().equals(username) && authenticatedSubject instanceof User) {
            User authenticatedUser = (User) authenticatedSubject;
            // Someone is trying to change a password that isn't there own. Is it an admin?
            if (!authenticatedUser.hasGroupRead(Group.ADMIN_KEY)) {
                LOG.info("Non-admin user {} attempted to change password for {}", authenticatedUser.getName(), username);
                return false;
            }
        }
        return true;
    }

}
