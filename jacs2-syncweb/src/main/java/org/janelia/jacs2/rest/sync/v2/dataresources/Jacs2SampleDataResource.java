package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.util.Date;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.GenericEntity;
import jakarta.ws.rs.core.Response;

import io.swagger.v3.oas.annotations.tags.Tag;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.dataservice.sample.SageDataService;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.jacs2.domain.sample.AnatomicalArea;
import org.janelia.model.jacs2.domain.sample.Sample;
import org.janelia.model.jacs2.page.ListResult;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.jacs2.sage.SlideImage;

@Tag(name = "SampleData", description = "JACS2 Sample data resource")
@RequireAuthentication
@ApplicationScoped
@Produces("application/json")
@Path("/samples")
public class Jacs2SampleDataResource {
    private static final int DEFAULT_PAGE_SIZE = 100;

    @Inject private SampleDataService sampleDataService;
    @Inject private SageDataService sageDataService;

    @GET
    public Response getAllSamples(
            @HeaderParam("authToken") String authToken,
            @QueryParam("sample-id") Long sampleId,
            @QueryParam("sample-name") String sampleName,
            @QueryParam("sample-owner") String sampleOwner,
            @QueryParam("age") String age,
            @QueryParam("effector") String effector,
            @QueryParam("dataset") String dataset,
            @QueryParam("line") String line,
            @QueryParam("slidecode") String slidecode,
            @QueryParam("gender") String gender,
            @QueryParam("status") String status,
            @QueryParam("tmog-from") Date tmogFrom,
            @QueryParam("tmog-to") Date tmogTo,
            @QueryParam("page") Integer pageNumber,
            @QueryParam("length") Integer pageLength) {
        Sample pattern = new Sample();
        pattern.setId(sampleId);
        pattern.setName(sampleName);
        pattern.setOwnerKey(sampleOwner);
        pattern.setAge(age);
        pattern.setEffector(effector);
        pattern.setDataSet(dataset);
        pattern.setLine(line);
        pattern.setSlideCode(slidecode);
        pattern.setGender(gender);
        pattern.setStatus(status);
        PageRequest pageRequest = new PageRequest();
        if (pageNumber != null) {
            pageRequest.setPageNumber(pageNumber);
        }
        if (pageLength != null) {
            pageRequest.setPageSize(pageLength);
        } else {
            pageRequest.setPageSize(DEFAULT_PAGE_SIZE);
        }
        PageResult<Sample> results = sampleDataService.searchSamples(extractSubjectFromAuthToken(authToken), pattern, new DataInterval<>(tmogFrom, tmogTo), pageRequest);
        return Response
                .status(Response.Status.OK)
                .entity(new GenericEntity<PageResult<Sample>>(results){})
                .build();
    }

    @GET
    @Path("/{sample-id}")
    public Sample getSample(@HeaderParam("authToken") String authToken, @PathParam("sample-id") Long sampleId) {
        return sampleDataService.getSampleById(extractSubjectFromAuthToken(authToken), sampleId);
    }

    @GET
    @Path("/{sample-id}/anatomicalAreas")
    public Response getAnatomicalArea(@HeaderParam("authToken") String authToken, @PathParam("sample-id") Long sampleId, @QueryParam("objective") String objectiveName) {
        List<AnatomicalArea> anatomicalAreas = sampleDataService.getAnatomicalAreasBySampleIdAndObjective(extractSubjectFromAuthToken(authToken), sampleId, objectiveName);
        return Response
                .status(Response.Status.OK)
                .entity(new ListResult<>(anatomicalAreas))
                .build();
    }

    @GET
    @Path("/sage")
    public Response getSageImages(@HeaderParam("authToken") String authToken,
                                  @QueryParam("dataset") String dataset,
                                  @QueryParam("line") String line,
                                  @QueryParam("slide-code") List<String> slideCodes,
                                  @QueryParam("lsm") List<String> lsmNames,
                                  @QueryParam("page") Integer pageNumber,
                                  @QueryParam("length") Integer pageLength) {
        PageRequest pageRequest = new PageRequest();
        if (pageNumber != null) {
            pageRequest.setPageNumber(pageNumber);
        }
        if (pageLength != null) {
            pageRequest.setPageSize(pageLength);
        } else {
            pageRequest.setPageSize(DEFAULT_PAGE_SIZE);
        }
        List<SlideImage> slideImages = sageDataService.getMatchingImages(dataset, line, slideCodes, lsmNames, pageRequest);
        return Response
                .status(Response.Status.OK)
                .entity(new ListResult<>(slideImages))
                .build();
    }

    private String extractSubjectFromAuthToken(String authToken) {
        return null; // Not implemented yet.
    }
}
