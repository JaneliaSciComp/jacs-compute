package org.janelia.jacs2.asyncservice.common;

import com.beust.jcommander.JCommander;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.model.service.ServiceMetaData;

import javax.inject.Named;

public class ServiceArgs {

    public static <A extends ServiceArgs> A parse(String[] argsList, A args) {
        new JCommander(args).parse(argsList);
        return args;
    }

    public static <A extends ServiceArgs> String usage(String serviceName, A args) {
        StringBuilder usageOutput = new StringBuilder();
        JCommander jc = new JCommander(args);
        jc.setProgramName(serviceName);
        jc.usage(usageOutput);
        return usageOutput.toString();
    }

    public static <P extends ServiceProcessor, A extends ServiceArgs> ServiceMetaData getMetadata(Class<P> processorClass, A args) {
        Named namedAnnotation = processorClass.getAnnotation(Named.class);
        Preconditions.checkArgument(namedAnnotation != null);
        String serviceName = namedAnnotation.value();
        ServiceMetaData smd = new ServiceMetaData();
        smd.setServiceName(serviceName);
        smd.setUsage(ServiceArgs.usage(serviceName, args));
        return smd;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
