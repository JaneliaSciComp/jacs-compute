package org.janelia.jacs2.asyncservice.utils;

import de.odysseus.el.ExpressionFactoryImpl;
import de.odysseus.el.util.SimpleContext;

import javax.el.ExpressionFactory;
import javax.el.PropertyNotFoundException;
import javax.el.ValueExpression;
import java.util.List;
import java.util.Map;

public class ExprEvalHelper {

    public static Object eval(String argExpr, Map<String, List<Object>> evalContext) {
        ExpressionFactory factory = new ExpressionFactoryImpl();
        SimpleContext context = new SimpleContext();
        evalContext.forEach((field, value) -> {
            if (value.size() == 1) {
                context.setVariable(field, factory.createValueExpression(value.get(0), Object.class));
            } else {
                context.setVariable(field, factory.createValueExpression(value, Object.class));
            }
        });
        ValueExpression argValExpr = factory.createValueExpression(context, argExpr, Object.class);
        try {
            Object argValue = argValExpr.getValue(context);
            if (argValue == null) {
                return argExpr;
            } else {
                return argValue;
            }
        } catch (PropertyNotFoundException e) {
            return argExpr;
        }
    }

}
