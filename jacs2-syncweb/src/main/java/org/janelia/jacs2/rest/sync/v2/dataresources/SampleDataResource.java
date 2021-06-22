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
import org.janelia.model.access.domain.dao.SampleDao;
import org.janelia.model.domain.DomainUtils;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.flyem.EMBody;
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
@Path("/data")
public class SampleDataResource {
    private static final Logger LOG = LoggerFactory.getLogger(SampleDataResource.class);

    @Inject
    private LegacyDomainDao legacyDomainDao;
    @Inject
    private SampleDao sampleDao;

    @ApiOperation(value = "Gets a list of matching samples by the name and/or slide code")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully got list of samples", response = Sample.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error getting list of Sample(s)")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/samples")
    public Response getSamples(@ApiParam @QueryParam("name") List<String> names,
                               @ApiParam @QueryParam("slideCode") List<String> slideCodes,
                               @ApiParam @QueryParam("offset") String offsetParam,
                               @ApiParam @QueryParam("length") String lengthParam) {
        LOG.trace("Start getSamples({}, {}, {}, {})", names, slideCodes, offsetParam, lengthParam);
        try {
            Set<String> sampleNames = extractMultiValueParams(names);
            Set<String> sampleSlideCodes = extractMultiValueParams(slideCodes);
            int offset = parseIntegerParam("offset", offsetParam, 0);
            int length = parseIntegerParam("length", lengthParam, -1);
            List<Sample> sampleList = sampleDao.findMatchingSample(null,
                    sampleNames,
                    sampleSlideCodes,
                    offset,
                    length);
            return Response
                    .ok(new GenericEntity<List<Sample>>(sampleList){})
                    .build();
        } catch (Exception e) {
            LOG.error("Error getting samples", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error retrieving samples"))
                    .build();
        } finally {
            LOG.trace("Finished getSamples({}, {}, {}, {})", names, slideCodes, offsetParam, lengthParam);
        }
    }

    @ApiOperation(value = "Gets a list of LSMImage stacks for a sample",
            notes = "Uses the sample ID"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully got list of LSMImage stacks", response = LSMImage.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error getting list of LSMImage Stacks")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/sample/lsms")
    public Response getLsmsForSample(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                     @ApiParam @QueryParam("sampleId") final Long sampleId,
                                     @ApiParam @QueryParam("sageSynced") @DefaultValue("true") final String sageSynced) {
        LOG.trace("Start getLsmsForSample({}, {})", subjectKey, sampleId);
        try {
            List<LSMImage> sampleLSMs;
            if ("true".equals(sageSynced)) {
                sampleLSMs = legacyDomainDao.getActiveLsmsBySampleId(subjectKey, sampleId);
            } else if ("false".equals(sageSynced)) {
                sampleLSMs = legacyDomainDao.getInactiveLsmsBySampleId(subjectKey, sampleId);
            } else if ("both".equals(sageSynced)) {
                sampleLSMs = legacyDomainDao.getAllLsmsBySampleId(subjectKey, sampleId);
            } else {
                LOG.error("Invalid value for sageSynced flag - {} for retrieving sample {} LSMs for {}", sageSynced, sampleId, subjectKey);
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorResponse("Invalid sageSynced flag " + sageSynced + " only support {false, true, both}"))
                        .build();
            }
            return Response
                    .ok(new GenericEntity<List<LSMImage>>(sampleLSMs){})
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred getting sample {} LSMs for {}", sampleId, subjectKey, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error retrieving sample " + sampleId + " LSMs"))
                    .build();
        } finally {
            LOG.trace("Finished getLsmsForSample({}, {})", subjectKey, sampleId);
        }
    }

    @ApiOperation(value = "Gets the secondary data files for an LSM within a sample",
            notes = "Uses the sample ID"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully got LSM secondary data files", response = Map.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error getting LSM secondary data files")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/sample/lsm")
    public Response getLSMByNameForSample(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                          @ApiParam @QueryParam("sampleId") final Long sampleId,
                                          @ApiParam @QueryParam("lsmName") final String lsmName) {
        LOG.trace("Start getLSMByNameForSample({}, {}, {})", subjectKey, sampleId, lsmName);
        try {
            for (LSMImage lsmImage : legacyDomainDao.getActiveLsmsBySampleId(subjectKey, sampleId)) {
                if (lsmImage.getName().startsWith(lsmName) || lsmName.startsWith(lsmImage.getName())) {
                    Map<FileType, String> files = getAbsoluteFiles(lsmImage);
                    return Response
                            .ok(new GenericEntity<Map<FileType, String>>(files){})
                            .build();
                }
            }

            return getBadRequest("LSM with name "+lsmName+" not found in Sample#"+sampleId);

        } catch (Exception e) {
            LOG.error("Error occurred getting sample {} lsm for {}", sampleId, subjectKey, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error retrieving sample " + sampleId + " lsm"))
                    .build();
        } finally {
            LOG.trace("Finished getLSMByNameForSample({}, {})", subjectKey, sampleId);
        }
    }

    @ApiOperation(value = "Gets the alignment data files for a result within a sample",
            notes = "Uses the sample ID"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully got alignment files", response = Map.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error getting alignment files")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/sample/alignment")
    public Response getAlignmentForSample(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                          @ApiParam @QueryParam("sampleId") final Long sampleId,
                                          @ApiParam @QueryParam("objective") final String objective,
                                          @ApiParam @QueryParam("area") final String area,
                                          @ApiParam @QueryParam("alignmentSpace") final String alignmentSpace) {
        LOG.trace("Start getAlignmentForSample({}, {}, {}, {}, {})", subjectKey, sampleId, objective, area, alignmentSpace);
        try {
            Sample sample = legacyDomainDao.getDomainObject(subjectKey, Sample.class, sampleId);
            if (sample==null) {
                LOG.error("Sample {} not found for {}", sampleId, subjectKey);
                return getBadRequest("Sample "+sampleId+" not found");
            }

            ObjectiveSample objectiveSample = sample.getObjectiveSample(objective);
            if (objectiveSample==null) {
                return getBadRequest("Objective "+objective+" not found in "+sample);
            }

            SamplePipelineRun latestSuccessfulRun = objectiveSample.getLatestSuccessfulRun();
            if (latestSuccessfulRun==null) {
                return getBadRequest("No processing results found for objective "+objective+" in "+sample);
            }

            List<Map<FileType, String>> results = latestSuccessfulRun.getAlignmentResults().stream()
                    .filter(s -> s.getAnatomicalArea().equals(area) && (alignmentSpace == null || s.getAlignmentSpace().equals(alignmentSpace)))
                    .map(this::getAbsoluteFiles)
                    .collect(Collectors.toList());

            return Response
                    .ok(new GenericEntity<List<Map<FileType, String>>>(results){})
                    .build();

        } catch (Exception e) {
            LOG.error("Error occurred getting sample {} secondary data for {}", sampleId, subjectKey, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error retrieving sample " + sampleId + " secondary data"))
                    .build();
        } finally {
            LOG.trace("Finished getAlignmentForSample({}, {})", subjectKey, sampleId);
        }
    }

    @ApiOperation(value = "Gets the primary alignment data files for a result within a sample",
            notes = "Uses the sample ID"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully got primary alignment files", response = FileType.class,
                    responseContainer = "Map"),
            @ApiResponse(code = 500, message = "Internal Server Error getting primary alignment files")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/sample/alignment/primary")
    public Response getPrimaryAlignmentForSample(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                                 @ApiParam @QueryParam("sampleId") final Long sampleId,
                                                 @ApiParam @QueryParam("objective") final String objective,
                                                 @ApiParam @QueryParam("area") final String area) {
        LOG.trace("Start getPrimaryAlignmentForSample({}, {}, {}, {})", subjectKey, sampleId, objective, area);
        try {
            Sample sample = legacyDomainDao.getDomainObject(subjectKey, Sample.class, sampleId);
            if (sample==null) {
                LOG.error("Sample {} not found for {}", sampleId, subjectKey);
                return getBadRequest("Sample "+sampleId+" not found");
            }

            ObjectiveSample objectiveSample = sample.getObjectiveSample(objective);
            if (objectiveSample==null) {
                return getBadRequest("Objective "+objective+" not found in "+sample);
            }

            SamplePipelineRun latestSuccessfulRun = objectiveSample.getLatestSuccessfulRun();
            if (latestSuccessfulRun==null) {
                return getBadRequest("No processing results found for objective "+objective+" in "+sample);
            }

            Map<FileType, String> results = latestSuccessfulRun.getAlignmentResults().stream()
                    .filter(s -> s.getAnatomicalArea().equals(area) && s.getBridgeParentAlignmentId()==null)
                    .map(this::getAbsoluteFiles)
                    .findFirst().orElse(Collections.emptyMap());

            return Response
                    .ok(new GenericEntity<Map<FileType, String>>(results){})
                    .build();

        } catch (Exception e) {
            LOG.error("Error occurred getting sample {} secondary data for {}", sampleId, subjectKey, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error retrieving sample " + sampleId + " secondary data"))
                    .build();
        } finally {
            LOG.trace("Finished getAlignmentForSample({}, {})", subjectKey, sampleId);
        }
    }

    @ApiOperation(value = "Gets the secondary data files for a result within a sample",
            notes = "Uses the sample ID and result filepath"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully got secondary data", response = FileType.class,
                    responseContainer = "Map"),
            @ApiResponse(code = 500, message = "Internal Server Error getting secondary data")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/sample/secondary")
    public Response getSecondaryDataForSample(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                              @ApiParam @QueryParam("sampleId") final Long sampleId,
                                              @ApiParam @QueryParam("objective") final String objective,
                                              @ApiParam @QueryParam("area") final String area,
                                              @ApiParam @QueryParam("tile") final String tile) {
        LOG.trace("Start getSecondaryDataForSample({}, {}, {}, {}, {})", subjectKey, sampleId, objective, area, tile);
        try {
            Sample sample = legacyDomainDao.getDomainObject(subjectKey, Sample.class, sampleId);
            if (sample==null) {
                LOG.error("Sample {} not found for {}", sampleId, subjectKey);
                return getBadRequest("Sample "+sampleId+" not found");
            }

            ObjectiveSample objectiveSample = sample.getObjectiveSample(objective);
            if (objectiveSample==null) {
                return getBadRequest("Objective "+objective+" not found in "+sample);
            }

            SamplePipelineRun latestSuccessfulRun = objectiveSample.getLatestSuccessfulRun();
            if (latestSuccessfulRun==null) {
                return getBadRequest("No processing results found for objective "+objective+" in "+sample);
            }

            SamplePostProcessingResult latestResultOfType = latestSuccessfulRun.getLatestResultOfType(SamplePostProcessingResult.class);
            if (latestResultOfType==null) {
                return getBadRequest("No post processing results found in "+sample);
            }

            FileGroup group;

            if (tile!=null) {
                group = latestResultOfType.getGroup(tile);
                if (group==null) {
                    return getBadRequest("No post processing results found for tile "+tile+" in "+sample);
                }
            }
            else {
                group = latestResultOfType.getGroup(area);
                if (group==null) {
                    return getBadRequest("No post processing results found for area "+area+" in "+sample);
                }
                // user wants secondary data for a stitched result, let's add the stack as well for convenience
                SampleProcessingResult result = latestSuccessfulRun.getSampleProcessingResults().stream()
                        .filter(s -> s.getAnatomicalArea().equals(area)).findFirst().orElse(null);
                if (result != null) {
                    group.getFiles().put(FileType.VisuallyLosslessStack, DomainUtils.getFilepath(result, FileType.VisuallyLosslessStack));
                }
            }

            Map<FileType, String> files = getAbsoluteFiles(group);
            return Response
                    .ok(new GenericEntity<Map<FileType, String>>(files){})
                    .build();

        } catch (Exception e) {
            LOG.error("Error occurred getting sample {} secondary data for {}", sampleId, subjectKey, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error retrieving sample " + sampleId + " secondary data"))
                    .build();
        } finally {
            LOG.trace("Finished getSecondaryDataForSample({}, {})", subjectKey, sampleId);
        }
    }

    private Map<FileType, String> getAbsoluteFiles(HasRelativeFiles hasRelativeFiles) {
        Map<FileType, String> absoluteFiles = new HashMap<>();
        for (FileType fileType : hasRelativeFiles.getFiles().keySet()) {
            absoluteFiles.put(fileType, DomainUtils.getFilepath(hasRelativeFiles, fileType));
        }
        return absoluteFiles;
    }


    private Response getBadRequest(String message) {
        return Response.status(Response.Status.BAD_REQUEST)
                .entity(new ErrorResponse(message))
                .build();
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
