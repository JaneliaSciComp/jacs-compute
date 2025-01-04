package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.GenericEntity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import com.google.common.base.Splitter;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.domain.dao.EmBodyDao;
import org.janelia.model.access.domain.dao.EmDataSetDao;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.flyem.EMBody;
import org.janelia.model.domain.flyem.EMDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(name = "EMData", description = "Janelia Workstation Domain Data")
@RequireAuthentication
@Path("/emdata")
public class EMDataResource {
    private static final Logger LOG = LoggerFactory.getLogger(EMDataResource.class);

    @Inject
    private EmBodyDao emBodyDao;
    @Inject
    private EmDataSetDao emDataSetDao;

    @Operation(summary = "Gets a list of EM bodies for an em dataset")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully got list of EM bodies"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error getting list of EM Bodies")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/dataset/{emDatasetName}/{emDatasetVersion}")
    public Response getEmBodiesForDataset(@Parameter @PathParam("emDatasetName") String emDatasetName,
                                          @Parameter @PathParam("emDatasetVersion") String emDatasetVersion,
                                          @Parameter @QueryParam("name") List<String> names,
                                          @Parameter @QueryParam("offset") String offsetParam,
                                          @Parameter @QueryParam("length") String lengthParam) {
        LOG.trace("Start getEmBodiesForDataset({}, {}, {}, {})", emDatasetName, emDatasetVersion, offsetParam, lengthParam);
        try {
            EMDataSet dataSet = emDataSetDao.getDataSetByNameAndVersion(emDatasetName, emDatasetVersion);
            if (dataSet == null) {
                LOG.warn("Could not find data set {}:{}", emDatasetName, emDatasetVersion);
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorResponse("No EM dataset found for " + emDatasetName + ":" + emDatasetVersion))
                        .build();
            }
            Set<String> emBodies = extractMultiValueParams(names);
            int offset = parseIntegerParam("offset", offsetParam, 0);
            int length = parseIntegerParam("length", lengthParam, -1);
            List<EMBody> emBodyList = emBodyDao.getBodiesWithNameForDataSet(dataSet, emBodies, offset, length);
            return Response
                    .ok(new GenericEntity<List<EMBody>>(emBodyList) {
                    })
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred getting EM bodies for {}:{} between {} and {}", emDatasetName, emDatasetVersion,
                    offsetParam, lengthParam, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error retrieving EM bodies for " + emDatasetName + ":" + emDatasetVersion))
                    .build();
        } finally {
            LOG.trace("Finished getEmBodiesForDataset({}, {}, {}, {})", emDatasetName, emDatasetVersion, offsetParam, lengthParam);
        }
    }

    @Operation(summary = "Gets a list of EM bodies")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully got list of EM bodies"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error getting list of EM Bodies")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/emBodies")
    public Response getEmBodies(@Parameter @QueryParam("refs") List<String> refs) {
        LOG.trace("Start getEmBodies({})", refs);
        try {
            Set<Long> emBodyIds = extractMultiValueParams(refs).stream()
                    .map(Reference::createFor)
                    .map(Reference::getTargetId)
                    .collect(Collectors.toSet());
            List<EMBody> emBodyList = emBodyDao.findByIds(emBodyIds);
            Set<Long> emDataSetIds = emBodyList.stream().map(emBody -> emBody.getDataSetRef().getTargetId()).collect(Collectors.toSet());
            Map<Reference, EMDataSet> indexedDataSets = emDataSetDao.findByIds(emDataSetIds).stream()
                    .collect(Collectors.toMap(Reference::createFor, emds -> emds));

            List<EMBody> emBodyListResult = emBodyList.stream()
                    .peek(emb -> emb.setEmDataSet(indexedDataSets.get(emb.getDataSetRef())))
                    .collect(Collectors.toList());

            return Response
                    .ok(new GenericEntity<List<EMBody>>(emBodyListResult) {
                    })
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred getting EM bodies for {}", refs, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error retrieving EM bodies for the provided " + refs.size() + " references"))
                    .build();
        } finally {
            LOG.trace("Finished getEmBodies({})", refs);
        }
    }

    private Set<String> extractMultiValueParams(List<String> params) {
        if (CollectionUtils.isEmpty(params)) {
            return Collections.emptySet();
        } else {
            return params.stream()
                    .filter(StringUtils::isNotBlank)
                    .flatMap(param -> Splitter.on(',').trimResults().omitEmptyStrings().splitToList(param).stream())
                    .collect(Collectors.toSet())
                    ;
        }
    }

    private Integer parseIntegerParam(String paramName, String paramValue, Integer defaultValue) {
        try {
            return StringUtils.isNotBlank(paramValue) ? Integer.parseInt(paramValue.trim()) : defaultValue;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid int value " + paramName + "->" + paramValue, e);
        }
    }

}
