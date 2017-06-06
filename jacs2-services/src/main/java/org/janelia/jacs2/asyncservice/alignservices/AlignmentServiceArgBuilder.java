package org.janelia.jacs2.asyncservice.alignservices;

import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.sampleprocessing.SampleProcessorResult;

import java.util.List;

public interface AlignmentServiceArgBuilder {
    ServiceArg[] getAlignerArgs(List<SampleProcessorResult> sampleResults);
}
