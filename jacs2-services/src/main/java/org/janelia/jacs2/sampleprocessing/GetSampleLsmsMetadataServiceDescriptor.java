package org.janelia.jacs2.sampleprocessing;

import org.janelia.jacs2.model.service.ServiceMetaData;
import org.janelia.jacs2.service.impl.ServiceDescriptor;

import javax.inject.Inject;
import javax.inject.Named;

@Named("getSampleLsmMetadata")
public class GetSampleLsmsMetadataServiceDescriptor implements ServiceDescriptor {
    private final GetSampleLsmsMetadataProcessor sampleLsmMetadataProcessor;

    @Inject
    GetSampleLsmsMetadataServiceDescriptor(GetSampleLsmsMetadataProcessor sampleLsmMetadataProcessor) {
        this.sampleLsmMetadataProcessor = sampleLsmMetadataProcessor;
    }

    @Override
    public ServiceMetaData getMetadata() {
        String serviceName = this.getClass().getAnnotation(Named.class).value();
        ServiceMetaData smd = new ServiceMetaData();
        smd.setServiceName(serviceName);
        smd.setUsage(SampleServiceArgs.usage(serviceName, new SampleServiceArgs()));
        return smd;
    }

    @Override
    public GetSampleLsmsMetadataProcessor createServiceProcessor() {
        return sampleLsmMetadataProcessor;
    }

}
