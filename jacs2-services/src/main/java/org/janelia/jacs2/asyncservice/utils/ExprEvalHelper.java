package org.janelia.jacs2.asyncservice.utils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import de.odysseus.el.ExpressionFactoryImpl;
import de.odysseus.el.util.SimpleContext;
import org.janelia.jacs2.cdi.ObjectMapperFactory;

import javax.el.ExpressionFactory;
import javax.el.PropertyNotFoundException;
import javax.el.ValueExpression;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ExprEvalHelper {

    public static String eval(String argExpr, List<Object> forwardedResults) {
        ObjectMapper objectMapper = ObjectMapperFactory.instance()
                .newMongoCompatibleObjectMapper()
                .setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
        ExpressionFactory factory = new ExpressionFactoryImpl();
        for (Object result : forwardedResults) {
            SimpleContext context = new SimpleContext();
            Map<String, Object> resultFields = convertObjectToMap(objectMapper, result);
            resultFields.forEach((field, value) -> {
                context.setVariable(field, factory.createValueExpression(value, Object.class));
            });
            ValueExpression argValExpr = factory.createValueExpression(context, argExpr, Object.class);
            try {
                Object argValue = argValExpr.getValue(context);
                if (argValue == null) {
                    continue;
                } else {
                    return argValue.toString();
                }
            } catch (PropertyNotFoundException e) {
                continue;
            }
        }
        return argExpr;
    }


    private static Map<String, Object> convertObjectToMap(ObjectMapper objectMapper, Object o) {
        JsonNode node = objectMapper.valueToTree(o);
        Object nodeFields = getNodeFields(node);
        Map<String, Object> objectFields = new LinkedHashMap<>();
        if (nodeFields instanceof String) {
            objectFields.put("result", nodeFields);
        } else if (nodeFields instanceof List) {
            objectFields.put("result", nodeFields);
        } else if (nodeFields instanceof Map) {
            objectFields.putAll((Map)nodeFields);
        }
        return objectFields;
    }

    private static Object getNodeFields(JsonNode node) {
        if (!node.isContainerNode()) {
            return node.asText();
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
}
