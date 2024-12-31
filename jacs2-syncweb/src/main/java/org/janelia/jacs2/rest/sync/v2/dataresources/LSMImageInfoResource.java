package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.util.Arrays;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.domain.dao.LSMImageDao;
import org.janelia.model.domain.sample.LSMImage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(name = "LSMImageInfo", description = "Janelia Workstation Line Release(s)")
@RequireAuthentication
@ApplicationScoped
@Path("/info")
public class LSMImageInfoResource {
    private static final Logger LOG = LoggerFactory.getLogger(LSMImageInfoResource.class);

    @Inject
    private LSMImageDao lsmImageDao;

    @Operation(summary = "Gets lsm image stack by name")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully found and returned image"),
            @ApiResponse(responseCode = "404", description = "LSM image not found"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error while looking up image")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("lsmstack/images")
    public Response getLSMImageStackByMatchingName(@Parameter @QueryParam("name") String name,
                                                   @Parameter @QueryParam("includeNonSynced") Boolean includeNonSynced) {
        LOG.trace("Start getLSMStackByName({})", name);
        try {
            if (StringUtils.isBlank(name)) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorResponse("Invalid image name"))
                        .build();
            }
            LSMImage lsmImage = findImagesByMatchingName(name).stream()
                    .filter(lsm -> includeNonSynced != null && includeNonSynced || lsm.isLSMSageSynced())
                    .findFirst()
                    .orElse(null);
            if (lsmImage == null) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No image " + name + "found"))
                        .build();
            } else {
                return Response
                        .ok(lsmImage)
                        .build();
            }
        } catch (Exception e) {
            LOG.error("Error occurred getting image {}", name, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error retrieving image " + name))
                    .build();
        } finally {
            LOG.trace("Finished getLSMStackByName({})", name);
        }
    }

    @Operation(summary = "Gets lsm stack by name")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully found and returned image"),
            @ApiResponse(responseCode = "404", description = "LSM image not found"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error while looking up image")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("lsmstack/name")
    public Response getLSMStackByName(@Parameter @QueryParam("name") String name,
                                      @Parameter @QueryParam("fuzzyMatch") Boolean fuzzyMatch,
                                      @Parameter @QueryParam("includeNonSynced") Boolean includeNonSynced) {
        LOG.trace("Start getLSMStackByName({})", name);
        try {
            if (StringUtils.isBlank(name)) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorResponse("Invalid image name"))
                        .build();
            }
            List<LSMImage> matchingImages;
            if (fuzzyMatch != null && fuzzyMatch) {
                matchingImages = findImagesByExactName(name);
            } else {
                matchingImages = findImagesByMatchingName(name);
            }
            LSMImage lsmImage = matchingImages.stream()
                    .filter(lsm -> includeNonSynced != null && includeNonSynced || lsm.isLSMSageSynced())
                    .findFirst()
                    .orElse(null);
            if (lsmImage == null) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No image " + name + "found"))
                        .build();
            } else {
                return Response
                        .ok(lsmImage)
                        .build();
            }
        } catch (Exception e) {
            LOG.error("Error occurred getting image {}", name, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error retrieving image " + name))
                    .build();
        } finally {
            LOG.trace("Finished getLSMStackByName({})", name);
        }
    }

    private List<LSMImage> findImagesByMatchingName(String name) {
        String imageName;
        String archivedImageName;
        if (StringUtils.endsWith(name, ".bz2")) {
            imageName = StringUtils.removeEnd(name, ".bz2");
            archivedImageName = name;
        } else {
            imageName = name;
            archivedImageName = name + ".bz2";
        }
        return lsmImageDao.findEntitiesMatchingAnyGivenName(Arrays.asList(imageName, archivedImageName));
    }

    private List<LSMImage> findImagesByExactName(String name) {
        return lsmImageDao.findEntitiesByExactName(name);
    }

}
