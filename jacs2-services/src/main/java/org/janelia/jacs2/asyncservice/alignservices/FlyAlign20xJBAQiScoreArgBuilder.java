package org.janelia.jacs2.asyncservice.alignservices;

import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.sampleprocessing.SampleProcessorResult;

import java.util.List;

public class FlyAlign20xJBAQiScoreArgBuilder implements AlignmentServiceArgBuilder {
    @Override
    public ServiceArg[] getAlignerArgs(List<SampleProcessorResult> sampleResults) {
        return new ServiceArg[] {
                new ServiceArg("-inputFile", sampleResults.get(0).getAreaFile())
        };
    }
}
