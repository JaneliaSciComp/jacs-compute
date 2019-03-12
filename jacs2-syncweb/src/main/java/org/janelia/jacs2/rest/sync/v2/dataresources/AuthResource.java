package org.janelia.jacs2.rest.sync.v2.dataresources;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.glassfish.jersey.server.ContainerRequest;
import org.janelia.jacs2.auth.JWTProvider;
import org.janelia.jacs2.auth.PasswordProvider;
import org.janelia.jacs2.auth.impl.AuthProvider;
import org.janelia.jacs2.rest.sync.v2.dataresources.dto.AuthenticationRequest;
import org.janelia.jacs2.rest.sync.v2.dataresources.dto.AuthenticationResponse;
import org.janelia.model.access.domain.dao.SubjectDao;
import org.janelia.model.security.Subject;
import org.janelia.model.security.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Self-contained authentication against the passwords stored in the Subject collection.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@ApplicationScoped
@Path("/")
public class AuthResource {
    private static final Logger LOG = LoggerFactory.getLogger(AuthResource.class);

    @Inject
    private JWTProvider jwtProvider;
    @Inject
    private AuthProvider authProvider;

    @ApiOperation(value = "Authenticate against user password",
            notes = "Generates a JWT token for use in subsequent requests"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully authenticated and generated JWT", response = AuthenticationResponse.class),
            @ApiResponse(code = 401, message = "Could not authenticate user due to missing user or mismatched password"),
            @ApiResponse(code = 500, message = "Internal Server Error authenticating user")
    })
    @POST
    @Path("/authenticate")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public AuthenticationResponse authenticate(AuthenticationRequest authReq) {
        LOG.trace("Authenticate({})", authReq.getUsername());
        try {
            User user = authProvider.authenticate(authReq.getUsername(), authReq.getPassword());
            if (user==null) {
                throw new WebApplicationException(Response.Status.UNAUTHORIZED);
            }

            // Generate JWT
            String token = jwtProvider.generateJWT(user);

            // Create response
            AuthenticationResponse authRes = new AuthenticationResponse();
            authRes.setUsername(authReq.getUsername());
            authRes.setToken(token);
            return authRes;

        } catch (Exception e) {
            if (e instanceof WebApplicationException) {
                throw (WebApplicationException)e;
            }
            else {
                LOG.error("Error occurred authenticating {}", authReq.getUsername(), e);
                throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
            }
        }
    }

    // TEMPORARY CODE REMOVE THIS!!!!
    @Inject
    private SubjectDao subjectDao;
    @Inject
    private PasswordProvider pwProvider;

    @ApiOperation(value = "Changes a user's password",
            notes = ""
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully set user password", response = User.class),
            @ApiResponse(code = 500, message = "Internal Server Error setting user preferences")
    })
    @POST
    @Path("/user/password")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public User changePassword(AuthenticationRequest authenticationMessage, @Context ContainerRequest containerRequestContext) {
        LOG.trace("changePassword({})", authenticationMessage.getUsername());
        try {

            Subject subject = subjectDao.findByName(authenticationMessage.getUsername());
            if (!(subject instanceof User)) {
                throw new WebApplicationException(Response.Status.NOT_FOUND);
            }
            User user = (User)subject;
            String passwordHash = pwProvider.generatePBKDF2Hash(authenticationMessage.getPassword());
            subjectDao.setUserPassword(user, passwordHash);
            return user;

        } catch (Exception e) {
            if (e instanceof WebApplicationException) {
                throw (WebApplicationException)e;
            }
            else {
                LOG.error("Error occurred changing password for user {}", authenticationMessage.getUsername(), e);
                throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
            }
        }
    }
}
