package org.janelia.jacs2.asyncservice.common;

import com.beust.jcommander.JCommander;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import org.janelia.jacs2.asyncservice.utils.ExprEvalHelper;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.jacs2.SetFieldValueHandler;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceArgDescriptor;
import org.janelia.model.service.ServiceMetaData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
            serviceMetaData.getServiceArgDescriptors().forEach((ServiceArgDescriptor sd) -> {
                String argName = sd.getArgName();
                Object argValue = prepareArgValue(sd.getArg().get(serviceMetaData.getServiceArgsObject()));
                serviceDataUpdates.putAll(serviceData.addServiceArg(argName, argValue));
            });
            serviceDataUpdates.putAll(serviceData.addServiceArgs(serviceData.getDictionaryArgs())); // add the dictionary args
        }
        return serviceDataUpdates;
    }

    private Object prepareArgValue(Object value) {
        if (value == null) {
            return null;
        }
        List<String> stringValues = argValueAsStream(value).flatMap(v -> argValueAsStream(v)).map(v -> v.toString()).collect(Collectors.toList());
        if (stringValues.size() < 1) {
            return null;
        } else if (stringValues.size() == 1) {
            return stringValues.get(0);
        } else {
            return stringValues;
        }
    }

    private Stream<Object> argValueAsStream(Object argValue) {
        if (argValue.getClass().isArray()) {
            return arrayValueAsStream((Object[]) argValue);
        } else if (argValue instanceof Iterable) {
            return iterableValueAsStream((Iterable) argValue);
        } else {
            return simpleValueAsStream(argValue);
        }
    }
    private Stream<Object> arrayValueAsStream(Object[] arrayValue) {
        return Stream.of(arrayValue);
    }

    private Stream<Object> iterableValueAsStream(Iterable<Object> iterableValue) {
        return Streams.stream(iterableValue);
    }

    private Stream<Object> simpleValueAsStream(Object simpleValue) {
        return Stream.of(simpleValue);
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
