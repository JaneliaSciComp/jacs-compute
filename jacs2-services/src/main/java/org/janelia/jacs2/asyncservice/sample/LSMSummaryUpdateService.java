package org.janelia.jacs2.asyncservice.sample;

import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor2;
import org.janelia.jacs2.asyncservice.sample.helpers.SampleHelper;
import org.janelia.jacs2.asyncservice.sample.helpers.SamplePipelineUtils;
import org.janelia.model.access.domain.DomainUtils;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.sample.*;
import org.janelia.model.domain.sample.pipeline.SingleLSMSummaryResult;
import org.janelia.model.service.JacsServiceData;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.Collection;

@Named("lsmSummaryUpdate")

@Service(description="Commit the LSM summary to the Sample")

@ServiceInput(name="lsmSummary",
        type=SingleLSMSummaryResult.class,
        description="LSM summary result",
        variadic = true)

@ServiceInput(name="sampleId",
        type=Long.class,
        description="Sample GUID")

@ServiceInput(name="pipelineRunId",
        type=Long.class,
        description="Pipeline Run GUID")

@ServiceOutput(
        name="sample",
        type=Sample.class,
        description="Updated sample")

public class LSMSummaryUpdateService extends AbstractBasicLifeCycleServiceProcessor2<Sample> {

    @Inject
    private Instance<SampleHelper> sampleHelperInstance;

    private final String resultName = "LSM Summary Result";

    @Override
    protected Sample execute(JacsServiceData sd) throws Exception {

        SampleHelper sampleHelper = sampleHelperInstance.get();
        sampleHelper.init(sd, logger);

        Collection<SingleLSMSummaryResult> lsmSummaryResults = getRequiredServiceInput(sd,"lsmSummary");
        Long sampleId = getRequiredServiceInput(sd,"sampleId");
        Long pipelineRunId = getRequiredServiceInput(sd,"pipelineRunId");

        logger.info("Got lsmSummaryResults={}", lsmSummaryResults);

        // Update the Sample with the new result
        Sample sample = sampleHelper.lockAndRetrieve(Sample.class, sampleId);
        try {
            SamplePipelineRun pipelineRun = sample.getRunById(pipelineRunId);

            LSMSummaryResult lsmSummaryResult = sampleHelper.addNewLSMSummaryResult(resultName);

            for (SingleLSMSummaryResult partialResult : lsmSummaryResults) {
                String prefix = SamplePipelineUtils.getLSMPrefix(partialResult);
                FileGroup group = new FileGroup(prefix);
                group.setFiles(partialResult.getFiles());
                lsmSummaryResult.addGroup(group);
            }

            logger.info("Adding results to {} in {}", pipelineRun, sample);
            sampleHelper.addResult(pipelineRun, lsmSummaryResult);
            sample = sampleHelper.saveSample(sample);
        }
        finally {
            sampleHelper.unlock(sample);
        }

        // Update the LSMs
        for (SingleLSMSummaryResult partialResult : lsmSummaryResults) {

            LSMImage lsm = sampleHelper.lockAndRetrieve(LSMImage.class, partialResult.getLsmId());
            try {

                if (partialResult.getBrightnessCompensation()!=null) {
                    lsm.setBrightnessCompensation(partialResult.getBrightnessCompensation());
                }

                if (partialResult.getChannelColors()!=null) {
                    lsm.setChannelColors(partialResult.getChannelColors());
                }

                if (partialResult.getChannelDyeNames()!=null) {
                    lsm.setChannelDyeNames(partialResult.getChannelDyeNames());
                }

                for (FileType fileType : partialResult.getFiles().keySet()) {

                    // Never update the real LSM filepath to our temporary one
                    if (fileType == FileType.LosslessStack) continue;

                    String value = DomainUtils.getFilepath(partialResult, fileType);
                    if (value!=null) {
                        DomainUtils.setFilepath(lsm, fileType, value);
                    }

                }

                logger.info("Adding results to {}", lsm);
                sampleHelper.saveLsm(lsm);
            }
            finally {
                sampleHelper.unlock(lsm);
            }
        }

        return sample;
    }
}
