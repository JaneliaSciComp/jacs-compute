package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.base.Splitter;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.EmBodyDao;
import org.janelia.model.access.domain.dao.EmDataSetDao;
import org.janelia.model.domain.DomainUtils;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.flyem.EMBody;
import org.janelia.model.domain.flyem.EMDataSet;
import org.janelia.model.domain.interfaces.HasRelativeFiles;
import org.janelia.model.domain.sample.FileGroup;
import org.janelia.model.domain.sample.LSMImage;
import org.janelia.model.domain.sample.ObjectiveSample;
import org.janelia.model.domain.sample.Sample;
import org.janelia.model.domain.sample.SamplePipelineRun;
import org.janelia.model.domain.sample.SamplePostProcessingResult;
import org.janelia.model.domain.sample.SampleProcessingResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
@ApplicationScoped
@Path("/emdata")
public class EMDataResource {
    private static final Logger LOG = LoggerFactory.getLogger(EMDataResource.class);

    @Inject
    private EmBodyDao emBodyDao;
    @Inject
    private EmDataSetDao emDataSetDao;

    @ApiOperation(value = "Gets a list of EM bodies for an em dataset")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully got list of EM bodies",
                         response = EMBody.class,
                         responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error getting list of EM Bodies")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/dataset/{emDatasetName}/{emDatasetVersion}")
    public Response getEmBodiesForDataset(@ApiParam @PathParam("emDatasetName") String emDatasetName,
                                          @ApiParam @PathParam("emDatasetVersion") String emDatasetVersion,
                                          @ApiParam @QueryParam("name") List<String> names,
                                          @ApiParam @QueryParam("offset") String offsetParam,
                                          @ApiParam @QueryParam("length") String lengthParam) {
        LOG.trace("Start getEmBodiesForDataset({}, {}, {}, {})", emDatasetName, emDatasetVersion, offsetParam, lengthParam);
        try {
            EMDataSet dataSet = emDataSetDao.getDataSetByNameAndVersion(emDatasetName, emDatasetVersion);
            if (dataSet==null) {
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
                    .ok(new GenericEntity<List<EMBody>>(emBodyList){})
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
