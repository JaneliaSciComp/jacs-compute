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
import org.janelia.jacs2.auth.LDAPProvider;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.DatasetDao;
import org.janelia.model.access.domain.dao.SubjectDao;
import org.janelia.model.domain.Preference;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.security.Subject;
import org.janelia.model.security.User;
import org.janelia.model.security.util.SubjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private LDAPProvider ldapProvider;

    @ApiOperation(value = "Gets or creates a user",
            notes = "The authenticated user must be the same as the user being retrieved"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully attempted user retrieval", response = User.class),
            @ApiResponse(code = 500, message = "Internal Server Error trying to get or create user")
    })
    @GET
    @Path("/user/getorcreate")
    @Produces(MediaType.APPLICATION_JSON)
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
                // if subject doesn't exist for this user, create the account
                User newUser = ldapProvider.getUserInfo(username);
                subjectDao.save(newUser);
                LOG.info("Created new user based on LDAP information: {}", newUser);
                return Response
                        .created(UriBuilder.fromMethod(UserResource.class, "getSubjectById").build(newUser.getId()))
                        .entity(newUser)
                        .build();
            } else {
                return Response.ok(existingUser)
                        .build();
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

    @ApiOperation(value = "Get a List of the Workstation Users")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully got list of workstation users", response = Subject.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error getting list of workstation users")
    })
    @GET
    @Path("/user/subjects")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
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
    @Path("/user/subjects/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
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
    @Path("/user/subject")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSubjectByNameOrKey(@QueryParam("subjectKey") String subjectNameOrKey) {
        LOG.trace("Start getSubjectByKey({})", subjectNameOrKey);
        try {
            Subject s = subjectDao.findByNameOrKey(subjectNameOrKey);
            if (s == null) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No subject found for " + subjectNameOrKey))
                        .build();
            } else {
                return Response.ok(s)
                        .build();
            }
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
    @Path("/user/preferences")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
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
    @Path("/user/preferences")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
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
    @Path("/user/permissions")
    @Consumes(MediaType.APPLICATION_JSON)
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
    @Path("/group/{groupKey:.*}/members")
    @Produces(MediaType.APPLICATION_JSON)
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
    @Path("/groups")
    @Produces(MediaType.APPLICATION_JSON)
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
    @Path("/group/{groupName:.*}/data_sets")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
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

}
