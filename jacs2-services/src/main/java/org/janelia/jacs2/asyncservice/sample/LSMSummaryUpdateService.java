package org.janelia.jacs2.asyncservice.sample;

import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor2;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.model.domain.sample.LSMSummaryResult;
import org.janelia.model.domain.sample.Sample;
import org.janelia.model.service.JacsServiceData;

import javax.inject.Named;

@Named("lsmSummaryUpdate")

@Service(description="Commit the LSM summary to the Sample")

@ServiceInput(name="result",
        type=LSMSummaryResult.class,
        description="LSM summary result")

@ServiceResult(
        name="sample",
        type=Sample.class,
        description="Updated sample")

public class LSMSummaryUpdateService extends AbstractServiceProcessor2<Sample> {

    @Override
    public ServiceComputation<JacsServiceResult<Sample>> process(JacsServiceData sd) {
        LSMSummaryResult result = new LSMSummaryResult();

        Sample sample = new Sample();

        return computationFactory.newCompletedComputation(updateServiceResult(sd, sample));
    }

}
