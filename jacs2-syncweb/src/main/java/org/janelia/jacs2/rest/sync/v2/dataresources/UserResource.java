package org.janelia.jacs2.rest.sync.v2.dataresources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.auth.JacsSecurityContextHelper;
import org.janelia.jacs2.auth.PasswordProvider;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.auth.impl.AuthProvider;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.DatasetDao;
import org.janelia.model.access.domain.dao.SubjectDao;
import org.janelia.model.access.domain.dao.WorkspaceNodeDao;
import org.janelia.model.domain.Preference;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.workspace.Workspace;
import org.janelia.model.security.Group;
import org.janelia.model.security.Subject;
import org.janelia.model.security.User;
import org.janelia.model.security.dto.AuthenticationRequest;
import org.janelia.model.security.UserGroupRole;
import org.janelia.model.security.util.SubjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SwaggerDefinition(
        securityDefinition = @SecurityDefinition(
                apiKeyAuthDefinitions = {
                        @ApiKeyAuthDefinition(key = "user", name = "username", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER),
                        @ApiKeyAuthDefinition(key = "runAs", name = "runasuser", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER)
                }
        )
)
@Api(
        value = "Janelia Workstation Domain Data",
        authorizations = {
                @Authorization("user"),
                @Authorization("runAs")
        }
)
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
    private AuthProvider authProvider;
    @Inject
    private WorkspaceNodeDao workspaceNodeDao;

    @ApiOperation(value = "Changes a user's password")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully set user password", response = User.class),
            @ApiResponse(code = 500, message = "Internal Server Error setting user preferences")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/user/password")
    public User changePassword(AuthenticationRequest authenticationMessage, @Context ContainerRequestContext containerRequestContext) {
        LOG.info("changePassword({})", authenticationMessage.getUsername());
        try {
            boolean isAllowed = checkAdministrationPrivileges(authenticationMessage.getUsername(), containerRequestContext);
            if (!isAllowed) {
                throw new WebApplicationException(Response.Status.FORBIDDEN);
            }

            Subject subject = subjectDao.findByName(authenticationMessage.getUsername());
            if (!(subject instanceof User)) {
                throw new WebApplicationException(Response.Status.NOT_FOUND);
            }
            User user = (User)subject;
            subjectDao.setUserPassword(user, pwProvider.generatePBKDF2Hash(authenticationMessage.getPassword()));
            return user;
        } catch (Exception e) {
            if (e instanceof WebApplicationException) {
                throw (WebApplicationException) e;
            } else {
                LOG.error("Error occurred changing password for user {}", authenticationMessage.getUsername(), e);
                throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
            }
        }
    }

    @ApiOperation(value = "Gets a user, creating the user if necessary",
            notes = "The authenticated user must be the same as the user being retrieved"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully attempted user retrieval", response = User.class),
            @ApiResponse(code = 500, message = "Internal Server Error trying to get or create user")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/user/getorcreate")
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
            Subject existingUser = subjectDao.findByNameOrKey(subjectKey);
            if (existingUser == null) {
                User user = authProvider.createUser(username);
                if (user!=null) {
                    return Response.ok(user).build();
                }
                return Response.status(Response.Status.NOT_FOUND).build();
            } else {
                return Response.ok(existingUser).build();
            }
        } catch (Exception e) {
            LOG.error("Error trying to get or create user for {}", subjectKey);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error getting or creating user for " + subjectKey))
                    .build();
        } finally {
            LOG.trace("Finished getOrCreateUser({})", subjectKey);
        }
    }

    @ApiOperation(value = "Updates a user details",
            notes = "The user id is used to search for the appropriate user"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated user", response = User.class),
            @ApiResponse(code = 500, message = "Internal Server Error trying to update user")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/user/property")
    public Response updateUser(@ApiParam Map<String, Object> userProperties, @Context ContainerRequestContext containerRequestContext) {
        User dbUser = null;
        try {
            LOG.info("Start updateUserProperty()");
            if (StringUtils.isBlank((String) userProperties.get("name"))) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorResponse("Invalid subject key " + userProperties.get("name")))
                        .build();
            }

            String username = (String)userProperties.get("name");
            boolean isAllowed = checkAdministrationPrivileges(username, containerRequestContext);
            if (!isAllowed) {
                throw new WebApplicationException(Response.Status.FORBIDDEN);
            }

            dbUser = (User) subjectDao.findByNameOrKey((String) userProperties.get("name"));
            // create new user
            if (dbUser == null) {
                User blankUser = authProvider.addUser(userProperties);

                if (blankUser != null) {
                    LOG.info("Created new user({}, {})", blankUser.getId(), blankUser.getKey());

                    // create home folder for user
                    Workspace newHomeWorkspace = new Workspace();
                    newHomeWorkspace.setName("Home");
                    newHomeWorkspace.setOwnerKey(blankUser.getKey());
                    LOG.info("Creating Home Folder for user {}", blankUser.getKey());
                    workspaceNodeDao.saveBySubjectKey(newHomeWorkspace, blankUser.getKey());

                    // assign
                    return Response.ok(blankUser).build();
                } else {
                    return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
                }
            } else {
                // existing user
                boolean emailR = subjectDao.updateUserProperty(dbUser, "email", (String) userProperties.get("email"));
                boolean fullNameR = subjectDao.updateUserProperty(dbUser, "fullName", (String) userProperties.get("fullname"));
                boolean nameR = subjectDao.updateUserProperty(dbUser, "name", (String) userProperties.get("name"));
                if (emailR && fullNameR && nameR)
                    return Response.status(Response.Status.OK).build();
                else
                    return Response.status(Response.Status.BAD_REQUEST).build();
            }

        }
        catch (Exception e) {
            LOG.error("Error trying to update user for {}", dbUser.getKey());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error trying to update user roles " + dbUser.getKey()))
                    .build();
        }
        finally {
           LOG.trace("Finished updateUserProperty");
        }
    }


    @ApiOperation(value = "Updates the user's group roles",
            notes = "The user id is used to search for the appropriate user"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated user property", response = User.class),
            @ApiResponse(code = 500, message = "Internal Server Error trying to update user")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/user/roles")
    public Response updateUserRoles(@ApiParam Set<UserGroupRole> roles,
                                    @ApiParam @QueryParam("userKey") String userKey) {
        Subject subject = subjectDao.findByNameOrKey(userKey);

        LOG.info("Start UpdateUser({}, {})", userKey, roles);
        if (subject==null) {
            LOG.error("Invalid user ({}) provided to updateUser", userKey);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse("Invalid user " + userKey))
                    .build();
        }
        try {
            User user = (User)subject;
            if (subjectDao.updateUserGroupRoles(user, roles))
                return Response.status(Response.Status.OK).build();
            else
                return Response.status(Response.Status.BAD_REQUEST).build();
        }
        catch (Exception e) {
            LOG.error("Error trying to update user for {}", userKey);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error trying to update user roles " + userKey))
                    .build();
        }
        finally {
            LOG.trace("Finished updateUserRoles({})", userKey);
        }
    }


    @ApiOperation(value = "Creates a Group",
            notes = "values given are the group's relevant properties"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully created new group", response = Group.class),
            @ApiResponse(code = 500, message = "Internal Server Error trying to create new group")
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/group")
    public Response createGroup(@ApiParam Group group) {
        String groupKey = group.getKey();
        LOG.trace("Start CreateGroup({})", groupKey);
        try {
            Group newGroup = legacyDomainDao.createGroup(group.getName(), group.getFullName());
            if (newGroup!=null)
                return Response.ok(newGroup).build();
            else
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity(new ErrorResponse("Error trying to create new group " + groupKey))
                        .build();
        }
        catch (Exception e) {
            LOG.error("Error trying to create new group " + groupKey);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error trying to create new group " + groupKey))
                    .build();
        }
        finally {
            LOG.trace("Finished createGroup({})", groupKey);
        }
    }

    @ApiOperation(value = "Get a List of the Workstation Users")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully got list of workstation users", response = Subject.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error getting list of workstation users")
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

    @ApiOperation(value = "Get a subject by their name or subject key")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully got subject", response = Subject.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error getting subject")
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

    @ApiOperation(value = "Get a subject by their name or subject key")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully got subject", response = Subject.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error getting subject")
    })
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/user/subject")
    public Response getSubjectByNameOrKey(@QueryParam("subjectKey") String subjectNameOrKey) {
        LOG.trace("Start getSubjectByKey({})", subjectNameOrKey);
        try {
            Subject s = subjectDao.findByNameOrKey(subjectNameOrKey);
            return Response.ok(s)
                    .build();
        } finally {
            LOG.trace("Finished getSubjectByKey({})", subjectNameOrKey);
        }
    }

    @ApiOperation(value = "Get a List of the User's Preferences")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully got user preferences", response = Preference.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error getting user preferences")
    })
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/user/preferences")
    public Response getPreferences(@ApiParam @QueryParam("subjectKey") String subjectKey) {
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

    @ApiOperation(value = "Sets User Preferences",
            notes = "uses the Preferences Parameter of the DomainQuery."
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully set user preferences", response = Preference.class),
            @ApiResponse(code = 500, message = "Internal Server Error setting user preferences")
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

    @ApiOperation(value = "Sets the permissions for a subject's access to a given domain object",
            notes = "uses a map (targetClass=domainObject class, granteeKey = user subject key, " +
                    "rights = read, write, targetId=Id of the domainObject"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully set user preferences"),
            @ApiResponse(code = 500, message = "Internal Server Error getting user preferences")
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/user/permissions")
    public void setPermissions(@ApiParam Map<String, Object> params) {
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

    @ApiOperation(value = "Gets a List of Members",
            notes = "Uses the group key to retrieve a list of members"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched the list of members", response = Subject.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error fetching the members")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/group/{groupKey:.*}/members")
    public List<Subject> getMembers(@ApiParam @PathParam("groupKey") final String groupKey) {
        LOG.trace("Start getMembers({})", groupKey);
        try {
            return subjectDao.getGroupMembers(groupKey);
        } finally {
            LOG.trace("Finished getMembers({})", groupKey);
        }
    }

    @ApiOperation(value = "Get a list of groups and number of users in each group")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully got list of groups", response = Map.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error getting list of groups")
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

    @ApiOperation(value = "Get a list of data sets a given group has access to read")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully got list of datasets", response = HashMap.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error getting list of datasets")
    })
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/group/{groupName:.*}/data_sets")
    public Map<String, String> getDataSets(@ApiParam @PathParam("groupName") final String groupName) {
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

    private boolean checkAdministrationPrivileges (String username, ContainerRequestContext containerRequestContext) {
        User authorizedSubject = JacsSecurityContextHelper.getAuthorizedUser(containerRequestContext);
        User authenticatedUser = JacsSecurityContextHelper.getAuthenticatedUser(containerRequestContext);

        if (authorizedSubject==null || authenticatedUser==null) {
            LOG.info("Unauthorized attempt to change password for {}", username);
            return false;
        }

        LOG.info("User {} is performing admin on user account for {}", authenticatedUser.getName(), username);
        if (!authorizedSubject.getName().equals(username)) {
            // Someone is trying to change a password that isn't there own. Is it an admin?
            if (!authenticatedUser.hasGroupRead(Group.ADMIN_KEY)) {
                LOG.info("Non-admin user {} attempted to change password for {}",
                        authenticatedUser.getName(), username);
                return false;
            }
        }
        return true;
    }

}
