package org.janelia.jacs2.asyncservice.common;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterDescription;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.model.service.ServiceArgDescriptor;
import org.janelia.model.service.ServiceMetaData;

import javax.inject.Named;
import java.util.List;
import java.util.stream.Collectors;

public class ServiceArgs {

    public static <A extends ServiceArgs> A parse(String[] argsList, A args) {
        new JCommander(args).parse(argsList);
        return args;
    }

    private static <A extends ServiceArgs> void populateArgumentDescriptors(A args, ServiceMetaData smd) {
        JCommander jc = new JCommander(args);
        List<ParameterDescription> parameterDescriptiontList = jc.getParameters();
        smd.setServiceArgs(args);
        smd.setDescription(args.getServiceDescription());
        smd.setServiceArgDescriptors(parameterDescriptiontList.stream()
                .filter(pd -> !pd.isHelp())
                .map(pd -> {
                    Parameter parameterAnnotation = pd.getParameterAnnotation();
                    return new ServiceArgDescriptor(
                            pd.getParameterized(),
                            parameterAnnotation.names(),
                            pd.getDefault(),
                            parameterAnnotation.arity(),
                            parameterAnnotation.required(),
                            pd.getDescription()
                    );
                })
                .collect(Collectors.toList())
        );
    }

    public static <P extends ServiceProcessor, A extends ServiceArgs> ServiceMetaData getMetadata(Class<P> processorClass, A args) {
        Named namedAnnotation = processorClass.getAnnotation(Named.class);
        Preconditions.checkArgument(namedAnnotation != null);
        String serviceName = namedAnnotation.value();
        return createMetadata(serviceName, args);
    }

    static <A extends ServiceArgs> ServiceMetaData createMetadata(String serviceName, A args) {
        ServiceMetaData smd = new ServiceMetaData();
        smd.setServiceName(serviceName);
        populateArgumentDescriptors(args, smd);
        return smd;
    }

    private final String serviceDescription;

    public ServiceArgs() {
        this(null);
    }

    public ServiceArgs(String serviceDescription) {
        this.serviceDescription = serviceDescription;
    }

    protected String getServiceDescription() {
        return serviceDescription;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
