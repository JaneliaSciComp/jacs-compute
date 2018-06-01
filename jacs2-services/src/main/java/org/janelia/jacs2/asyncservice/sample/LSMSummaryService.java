package org.janelia.jacs2.asyncservice.sample;

import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor2;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.model.domain.sample.LSMSummaryResult;
import org.janelia.model.domain.workflow.WorkflowImage;
import org.janelia.model.service.JacsServiceData;

import javax.inject.Named;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

@Named("lsmSummary")

@Service(description="Summarize LSMs by producing MIPs, movies, and metadata files")

@ServiceInput(name="inputImages",
        type=WorkflowImage.class,
        description="Primary LSM image",
        variadic=true)

@ServiceResult(
        name="outputResult",
        type=LSMSummaryResult.class,
        description="Summary result")

public class LSMSummaryService extends AbstractServiceProcessor2<LSMSummaryResult> {

    @Override
    public ServiceComputation<JacsServiceResult<LSMSummaryResult>> process(JacsServiceData sd) {

        Path serviceFolder = getServiceFolder(sd);
        Map<String, Object> args = sd.getDictionaryArgs();
        List<WorkflowImage> inputImages = (List<WorkflowImage>)args.get("inputImages");

        logger.info("Got {} input images", inputImages.size());
        for (WorkflowImage inputImage : inputImages) {
            logger.info("Got input: {}", inputImage.getName());
        }

        LSMSummaryResult result = new LSMSummaryResult();

        return computationFactory.newCompletedComputation(updateServiceResult(sd, result));
    }
}
