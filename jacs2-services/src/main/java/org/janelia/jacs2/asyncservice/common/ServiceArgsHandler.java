package org.janelia.jacs2.asyncservice.common;

import com.beust.jcommander.JCommander;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.janelia.jacs2.asyncservice.utils.ExprEvalHelper;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.jacs2.domain.IndexedReference;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceArgDescriptor;
import org.janelia.model.service.ServiceMetaData;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class ServiceArgsHandler {
    private final JacsServiceDataPersistence jacsServiceDataPersistence;
    private final ObjectMapper objectMapper;

    ServiceArgsHandler(JacsServiceDataPersistence jacsServiceDataPersistence) {
        this.jacsServiceDataPersistence = jacsServiceDataPersistence;
        objectMapper = ObjectMapperFactory.instance()
                .newMongoCompatibleObjectMapper()
                .setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    /**
     * Update all list arguments and the serviceArgs dictionary with the actual argument values - which replace
     * the placeholders for dependencies' results with their actual value.
     * The argument format that contains a result of a dependency is "|>${serviceName_serviceId.fieldName}"
     *
     * @param serviceMetaData
     * @param serviceData
     * @return
     */
    Map<String, EntityFieldValueHandler<?>> updateServiceArgs(ServiceMetaData serviceMetaData, JacsServiceData serviceData) {
        Map<String, EntityFieldValueHandler<?>> serviceDataUpdates = new LinkedHashMap<>();

        // lazily instantiate actual service invocation arguments
        if (serviceData.getActualArgs() == null) {
            JacsServiceArgs actualServiceArgs = evalJacsServiceArgs(serviceData);
            serviceDataUpdates.putAll(serviceData.updateActualListArgs(actualServiceArgs.getListArgs()));

            // update service arguments arguments - this involves creating a dictionary that contains both
            // the parsed actual arguments and the actual dictionary arguments
            JCommander cmdLineParser = new JCommander(serviceMetaData.getServiceArgsObject());
            cmdLineParser.parse(actualServiceArgs.getListArgsAsArray()); // parse the actual list service args
            // Collectors.toMap cannot have null values so
            // I had to use the alternative collect that takes a supplier, accumulator and combiner
            Map<String, Object> serviceArgsUpdates = serviceMetaData.getServiceArgDescriptors().stream()
                    .map((ServiceArgDescriptor sd) -> ImmutablePair.of(sd.getArgName(), prepareArgValue(sd.getArg().get(serviceMetaData.getServiceArgsObject()))))
                    .collect(HashMap::new, (m, pv) -> m.put(pv.getLeft(), pv.getRight()), Map::putAll)
                    ;
            serviceArgsUpdates.putAll(actualServiceArgs.getDictArgs()); // add the dictionary args
            serviceDataUpdates.putAll(serviceData.addServiceArgs(serviceArgsUpdates));
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
            @SuppressWarnings("unchecked")
            Iterable<Object> iterableArgValue = (Iterable<Object>) argValue;
            return iterableValueAsStream(iterableArgValue);
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

    private JacsServiceArgs evalJacsServiceArgs(JacsServiceData jacsServiceData) {
        // the forwarded arguments are of the form: |>${serviceId.fieldname} where
        // fieldname is a field name from the result of serviceId.
        Function<String, Optional<String>> argExprExtractor = arg -> arg == null || !arg.startsWith("|>")
                ? Optional.empty()
                : Optional.of(arg.substring(2));
        Predicate<String> isForwardedArg = arg -> argExprExtractor.apply(arg).isPresent();

        boolean forwardedListArgsFound = jacsServiceData.getArgs().stream().anyMatch(isForwardedArg);
        boolean forwardedDictArgsFound = streamValues(jacsServiceData.getDictionaryArgs())
                .filter(v -> v != null)
                .filter(v -> v instanceof String)
                .map(v -> (String) v)
                .anyMatch(isForwardedArg);

        Map<String, List<Object>> argExprEvalContext;
        if (forwardedListArgsFound || forwardedDictArgsFound) {
            argExprEvalContext = jacsServiceDataPersistence.findServiceHierarchy(jacsServiceData).serviceHierarchyStream()
                    .filter(sd -> sd.getSerializableResult() != null)
                    .map(sd -> ImmutablePair.of(sd.getName(), convertServiceResultToMap(sd)))
                    .collect(Collectors.groupingBy(e -> e.getLeft(), Collectors.mapping(e -> e.getRight(), Collectors.toList())));
        } else {
            argExprEvalContext = ImmutableMap.of();
        }
        List<String> actualServiceListArgs;
        if (forwardedListArgsFound) {
            actualServiceListArgs = jacsServiceData.getArgs().stream()
                    .map(arg -> argExprExtractor.apply(arg)
                            .map(expr -> ExprEvalHelper.eval(expr, argExprEvalContext))
                            .map(objVal -> objVal.toString())
                            .orElse(arg))
                    .collect(Collectors.toList());
        } else {
            actualServiceListArgs = ImmutableList.copyOf(jacsServiceData.getArgs());
        }
        Map<String, Object> actualServiceDictArgs;
        if (forwardedDictArgsFound) {
            actualServiceDictArgs = new LinkedHashMap<>();
            jacsServiceData.getDictionaryArgs()
                    .forEach((k, v) -> actualServiceDictArgs.put(k, evalValExpr(v, argExprExtractor, argExprEvalContext)));
        } else {
            actualServiceDictArgs = jacsServiceData.getDictionaryArgs();
        }

        return new JacsServiceArgs(actualServiceListArgs, actualServiceDictArgs);
    }

    @SuppressWarnings("unchecked")
    private Object convertServiceResultToMap(JacsServiceData sd) {
        JsonNode serviceResultAsJson = objectMapper.valueToTree(sd.getSerializableResult());
        return getNodeFields(serviceResultAsJson);
    }

    private Object getNodeFields(JsonNode node) {
        if (!node.isContainerNode()) {
            if (node.isBoolean()) {
                return node.asBoolean();
            } else if (node.isNumber()) {
                return node.numberValue();
            } else {
                return node.asText();
            }
        } else if (node.isArray()) {
            List<Object> nodeItems = new ArrayList<>();
            node.elements().forEachRemaining(arrElem -> {
                nodeItems.add(getNodeFields(arrElem));
            });
            return nodeItems;
        } else { // isObject
            Map<String, Object> objectFields = new LinkedHashMap<>();
            node.fields().forEachRemaining(fieldEntry -> {
                objectFields.put(fieldEntry.getKey(), getNodeFields(fieldEntry.getValue()));
            });
            return objectFields;
        }
    }

    private Stream<Object> streamValues(Object o) {
        if (o == null) {
            return Stream.empty();
        } else if (o.getClass().isArray() || Map.class.isAssignableFrom(o.getClass()) || Collection.class.isAssignableFrom(o.getClass())) {
            if (o.getClass().isArray()) {
                Object[] objArray = castToArray(o);
                return Arrays.asList(objArray).stream().flatMap(entry -> streamValues(entry));
            } else if (Collection.class.isAssignableFrom(o.getClass())) {
                @SuppressWarnings("unchecked")
                Collection<Object> objCollection = (Collection<Object>) o;
                return objCollection.stream().flatMap(entry -> streamValues(entry));
            } else {
                @SuppressWarnings("unchecked")
                Map<String, Object> objMap = (Map<String, Object>) o;
                return objMap.values().stream().flatMap(entry -> streamValues(entry));
            }
        } else {
            return Stream.of(o);
        }
    }

    private Object[] castToArray(Object o) {
        Object[] objArray;
        if (o.getClass().getComponentType().isPrimitive()) {
            int arrayLength = Array.getLength(o);
            objArray = new Object[arrayLength];
            for(int i = 0; i < arrayLength; i++){
                objArray[i] = Array.get(o, i);
            }
        } else {
            objArray = (Object[]) o;
        }
        return objArray;
    }

    private Object evalValExpr(Object val, Function<String, Optional<String>> argExprExtractor, Map<String, List<Object>> evalContext) {
        if (val == null) {
            return null;
        } else if (val.getClass().isArray()) {
            Object[] valArray = castToArray(val);
            IndexedReference.indexArrayContent(valArray, (i, o) -> new IndexedReference<>(o, i))
                    .forEach(indexedVal -> valArray[indexedVal.getPos()] = evalValExpr(indexedVal.getReference(), argExprExtractor, evalContext));
            return valArray;
        } else if (Collection.class.isAssignableFrom(val.getClass())) {
            @SuppressWarnings("unchecked")
            Collection<Object> valCollection = (Collection<Object>) val;
            return valCollection.stream().map(valEntry -> evalValExpr(valEntry, argExprExtractor, evalContext)).collect(Collectors.toList());
        } else if (Map.class.isAssignableFrom(val.getClass())) {
            @SuppressWarnings("unchecked")
            Map<String, Object> valMap = (Map<String, Object>) val;
            return valMap.entrySet().stream()
                    .collect(Collectors.toMap(
                            (Map.Entry<String, Object> valEntry) -> valEntry.getKey(),
                            (Map.Entry<String, Object> valEntry) -> evalValExpr(valEntry.getValue(), argExprExtractor, evalContext)));
        } else if (val instanceof String) {
            String sval = (String) val;
            return argExprExtractor.apply(sval)
                    .map(expr -> ExprEvalHelper.eval(expr, evalContext))
                    .orElse(val);
        } else {
            return val;
        }

    }
}
