package org.janelia.jacs2.asyncservice.common;

import com.beust.jcommander.JCommander;
import com.google.common.collect.ImmutableList;
import org.janelia.jacs2.asyncservice.utils.ExprEvalHelper;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.jacs2.SetFieldValueHandler;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

class ServiceArgsHandler {
    private final JacsServiceDataPersistence jacsServiceDataPersistence;

    ServiceArgsHandler(JacsServiceDataPersistence jacsServiceDataPersistence) {
        this.jacsServiceDataPersistence = jacsServiceDataPersistence;
    }

    Map<String, EntityFieldValueHandler<?>> updateServiceArgs(ServiceMetaData serviceMetaData, JacsServiceData serviceData) {
        Map<String, EntityFieldValueHandler<?>> serviceDataUpdates = new HashMap<>();

        // lazily instantiate actual service invocation arguments
        if (serviceData.getActualArgs() == null) {
            List<String> actualServiceArgs = evalJacsServiceArgs(serviceData);
            serviceDataUpdates.putAll(serviceData.updateActualArgs(actualServiceArgs));
            // update service arguments arguments - this involves creating a dictionary that contains both
            // the parsed actual arguments and the dictionary arguments
            JCommander cmdLineParser = new JCommander(serviceMetaData.getServiceArgsObject());
            cmdLineParser.parse(actualServiceArgs.toArray(new String[actualServiceArgs.size()])); // parse the actual service args
            serviceMetaData.getServiceArgDescriptors().forEach(sd -> {
                serviceDataUpdates.putAll(serviceData.addServiceArg(sd.getArgName(), sd.getArg().get(serviceMetaData.getServiceArgsObject())));
            });
            serviceDataUpdates.putAll(serviceData.addServiceArgs(serviceData.getDictionaryArgs())); // add the dictionary args
        }
        return serviceDataUpdates;
    }

    private List<String> evalJacsServiceArgs(JacsServiceData jacsServiceData) {
        // the forwarded arguments are of the form: |>${fieldname} where fieldname is a field name from the result.
        Predicate<String> isForwardedArg = arg -> arg != null && arg.startsWith("|>");
        boolean forwardedArgumentsFound = jacsServiceData.getArgs().stream().anyMatch(isForwardedArg);
        List<String> actualServiceArgs;
        if (!forwardedArgumentsFound) {
            actualServiceArgs = ImmutableList.copyOf(jacsServiceData.getArgs());
        } else {
            List<JacsServiceData> serviceDependencies = jacsServiceDataPersistence.findServiceDependencies(jacsServiceData);
            List<Object> serviceDependenciesResults = serviceDependencies.stream()
                    .filter(sd -> sd.getSerializableResult() != null)
                    .map(sd -> sd.getSerializableResult())
                    .collect(Collectors.toList());
            actualServiceArgs = jacsServiceData.getArgs().stream().map(arg -> {
                if (isForwardedArg.test(arg)) {
                    return ExprEvalHelper.eval(arg.substring(2), serviceDependenciesResults);
                } else {
                    return arg;
                }
            }).collect(Collectors.toList());
        }
        return actualServiceArgs;
    }

}
