package org.janelia.jacs2.rest.sync.v2.dataresources;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.janelia.jacs2.auth.JWTProvider;
import org.janelia.jacs2.auth.impl.AuthProvider;
import org.janelia.jacs2.user.UserManager;
import org.janelia.model.access.domain.dao.SubjectDao;
import org.janelia.model.security.User;
import org.janelia.model.security.dto.AuthenticationRequest;
import org.janelia.model.security.dto.AuthenticationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authentication service which delegates to an AuthProvider for the real work.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
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

    @Operation(summary = "Authenticate a user against the configured auth provider",
            description = "Generates a JWT token for use in subsequent requests"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully authenticated and generated JWT"),
            @ApiResponse(responseCode = "401", description = "Could not authenticate user due to missing user or mismatched password"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error authenticating user")
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
