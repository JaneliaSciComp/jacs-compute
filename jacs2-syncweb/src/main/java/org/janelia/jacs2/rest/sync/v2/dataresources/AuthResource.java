package org.janelia.jacs2.rest.sync.v2.dataresources;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.janelia.jacs2.auth.JWTProvider;
import org.janelia.jacs2.auth.impl.AuthProvider;
import org.janelia.jacs2.user.UserManager;
import org.janelia.model.access.domain.dao.SubjectDao;
import org.janelia.model.security.dto.AuthenticationResponse;
import org.janelia.model.security.dto.AuthenticationRequest;
import org.janelia.model.security.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Authentication service which delegates to an AuthProvider for the real work.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@ApplicationScoped
@Path("/auth")
public class AuthResource {
    private static final Logger LOG = LoggerFactory.getLogger(AuthResource.class);

    @Inject
    private JWTProvider jwtProvider;
    @Inject
    private AuthProvider authProvider;
    @Inject
    private SubjectDao subjectDao;
    @Inject
    private UserManager userManager;

    @ApiOperation(value = "Authenticate a user against the configured auth provider",
            notes = "Generates a JWT token for use in subsequent requests"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully authenticated and generated JWT", response = AuthenticationResponse.class),
            @ApiResponse(code = 401, message = "Could not authenticate user due to missing user or mismatched password"),
            @ApiResponse(code = 500, message = "Internal Server Error authenticating user")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/authenticate")
    public AuthenticationResponse authenticate(AuthenticationRequest authReq) {
        LOG.info("Authenticate({})", authReq.getUsername());
        try {
            // Authenticate user against the auth provider
            User userMetadata = authProvider.authenticate(authReq.getUsername(), authReq.getPassword());
            if (userMetadata==null) {
                throw new WebApplicationException(Response.Status.UNAUTHORIZED);
            }

            // Find user in the local database
            User user = subjectDao.findUserByNameOrKey(userMetadata.getName());
            if (user == null) {
                // User was able to authenticate, but doesn't exist yet in our local database.
                // Create user automatically
                user = userManager.createUser(userMetadata);
                LOG.info("Created user automatically from remote auth source: {}", user.getName());
            }

            // Generate JWT
            String token = jwtProvider.encodeJWT(user);

            // Create response
            AuthenticationResponse authRes = new AuthenticationResponse();
            authRes.setUsername(authReq.getUsername());
            authRes.setToken(token);
            return authRes;
        }
        catch (Exception e) {
            if (e instanceof WebApplicationException) {
                throw (WebApplicationException)e;
            } else {
                LOG.error("Error occurred authenticating {}", authReq.getUsername(), e);
                throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
            }
        }
    }
}
