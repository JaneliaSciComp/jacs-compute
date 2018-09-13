package org.janelia.jacs2.asyncservice.sample;

import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor2;
import org.janelia.jacs2.asyncservice.sample.helpers.SampleHelper;
import org.janelia.jacs2.asyncservice.sample.helpers.SamplePipelineUtils;
import org.janelia.model.domain.sample.*;
import org.janelia.model.domain.sample.pipeline.SingleLSMSummaryResult;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Collection;

@Named("combineLsmSummaries")

@Service(description="Create a combined LSM summary")

@ServiceInput(name="lsmSummary",
        type=SingleLSMSummaryResult.class,
        description="LSM summary results",
        variadic = true)

@ServiceOutput(
        name="lsmSummary",
        type=LSMSummaryResult.class,
        description="Combined LSM summary result")

public class CombineLSMSummaries extends AbstractBasicLifeCycleServiceProcessor2<LSMSummaryResult> {

    @Inject
    private SampleHelper sampleHelper;

    private final String resultName = "LSM Summary Result";

    @Override
    protected LSMSummaryResult createResult() {

        Collection<SingleLSMSummaryResult> lsmSummaryResults = getRequiredServiceInput("lsmSummary");

        LSMSummaryResult lsmSummaryResult = sampleHelper.createLSMSummaryResult(resultName);

        for (SingleLSMSummaryResult partialResult : lsmSummaryResults) {
            String prefix = SamplePipelineUtils.getLSMPrefix(partialResult);
            FileGroup group = new FileGroup(prefix);
            group.setFiles(partialResult.getFiles());
            lsmSummaryResult.addGroup(group);
            lsmSummaryResult.getBrightnessCompensation().put(prefix, partialResult.getBrightnessCompensation());
        }

        return lsmSummaryResult;
    }
}
