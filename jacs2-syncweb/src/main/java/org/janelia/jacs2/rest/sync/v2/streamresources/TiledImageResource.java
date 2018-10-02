package org.janelia.jacs2.rest.sync.v2.streamresources;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.rest.ErrorResponse;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.File;
import java.nio.file.Files;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

@ApplicationScoped
@Produces("application/json")
@Path("/tiled_images")
public class TiledImageResource {

    @Inject
    private Logger logger;

    @ApiOperation(value = "Tiff Stream", notes = "Streams the requested tile stored as a TIFF file")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    @GET
    @Path("tileAsTiffStream")
    @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
    public Response streamTIFFTile(@QueryParam("path_hint") String pathHint) {
        return getTileTIFFFile(pathHint)
                .map(tileFile -> {
                    StreamingOutput tileStream = output -> {
                        Files.copy(tileFile.toPath(), output);
                    };
                    return Response.ok(tileStream, MediaType.APPLICATION_OCTET_STREAM)
                            .header("Content-Length", tileFile.length())
                            .build();
                })
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No file found for " + pathHint))
                        .build())
                ;
    }

    private Optional<File> getTileTIFFFile(String pathHint) {
        File candidateFile = new File(pathHint);
        if (candidateFile.exists()) {
            return Optional.of(candidateFile);
        } else {
            String candidateFileName = candidateFile.getName();
            int extensionSeparatorPos = candidateFileName.lastIndexOf(".");
            String candidateFileExt;
            if (extensionSeparatorPos > -1) {
                candidateFileExt = candidateFileName.substring(extensionSeparatorPos);
            } else {
                candidateFileExt = "";
            }
            File parentDir = candidateFile.getParentFile();
            if (!parentDir.isDirectory()) {
                return Optional.empty();
            } else {
                File[] childFiles = parentDir.listFiles();
                if (childFiles == null || childFiles.length == 0) {
                    logger.warn("Parent of suggested path {} - {} is not a directory", pathHint, parentDir);
                    return Optional.empty();
                }
                Predicate<File> filterPredicate;
                if (StringUtils.isBlank(candidateFileExt)) {
                    filterPredicate = f -> f.getName().endsWith(candidateFileExt);
                } else {
                    filterPredicate = f -> f.getName().endsWith(".tif") || f.getName().endsWith(".tiff");
                }
                return Stream.of(childFiles)
                        .filter(filterPredicate)
                        .findFirst();
            }
        }
    }
}
