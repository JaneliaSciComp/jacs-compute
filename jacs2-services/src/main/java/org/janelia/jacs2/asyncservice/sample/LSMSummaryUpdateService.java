package org.janelia.jacs2.asyncservice.sample;

import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor2;
import org.janelia.jacs2.asyncservice.sample.helpers.SampleHelper;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.model.access.domain.DomainDAO;
import org.janelia.model.access.domain.DomainUtils;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.sample.*;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Map;

@Named("lsmSummaryUpdate")

@Service(description="Commit the LSM summary to the Sample")

@ServiceInput(name="lsmSummary",
        type=LSMSummaryResult.class,
        description="LSM summary result")

@ServiceInput(name="sampleId",
        type=Long.class,
        description="GUID of the Sample to update")

@ServiceInput(name="pipelineRunId",
        type=Long.class,
        description="GUID of the SamplePipelineRun to update")

@ServiceOutput(
        name="sample",
        type=Sample.class,
        description="Updated Sample")

public class LSMSummaryUpdateService extends AbstractBasicLifeCycleServiceProcessor2<Sample> {

    @Inject
    protected DomainDAO domainDao;

    @Inject
    private SampleHelper sampleHelper;

    @Override
    protected Sample createResult() throws Exception {

        LSMSummaryResult lsmSummaryResult = getRequiredServiceInput("lsmSummary");
        Long sampleId = getRequiredServiceInput("sampleId");
        Long pipelineRunId = getRequiredServiceInput("pipelineRunId");

        // Update the Sample with the new result
        Sample sample = sampleHelper.lockAndRetrieve(Sample.class, sampleId);
        try {
            SamplePipelineRun pipelineRun = sample.getRunById(pipelineRunId);
            logger.info("Adding results to {} in {}", pipelineRun, sample);
            sampleHelper.addResult(pipelineRun, lsmSummaryResult);
            sample = sampleHelper.saveSample(sample);
        }
        finally {
            sampleHelper.unlock(sample);
        }

        // Update the LSMs (denormalized info)
        for (LSMImage lsmImage : domainDao.getActiveLsmsBySampleId(currentService.getOwnerKey(), sampleId)) {
            String prefix = FileUtils.getFileName(lsmImage.getName());

            FileGroup lsmResult = lsmSummaryResult.getGroup(prefix);
            if (lsmResult!=null) {
                LSMImage lsm = sampleHelper.lockAndRetrieve(LSMImage.class, lsmImage.getId());
                try {

                    String brightnessCompensation = lsmSummaryResult.getBrightnessCompensation().get(prefix);
                    if (brightnessCompensation!=null) {
                        logger.info("Updating {} with brightnessCompensation={}", brightnessCompensation, lsm);
                        lsm.setBrightnessCompensation(brightnessCompensation);
                    }

                    for (FileType fileType : lsmResult.getFiles().keySet()) {

                        // Never update the real LSM filepath to our temporary one
                        if (fileType == FileType.LosslessStack) continue;

                        String value = DomainUtils.getFilepath(lsmResult, fileType);
                        if (value != null) {
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
        }

        return sample;
    }
}
