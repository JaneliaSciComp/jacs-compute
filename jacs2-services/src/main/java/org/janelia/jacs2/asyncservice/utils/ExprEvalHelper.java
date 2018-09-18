package org.janelia.jacs2.asyncservice.utils;

import javax.el.ELContext;
import javax.el.ELManager;
import javax.el.ELProcessor;
import javax.el.ExpressionFactory;
import javax.el.PropertyNotFoundException;
import javax.el.ValueExpression;
import java.util.List;
import java.util.Map;

public class ExprEvalHelper {

    public static Object eval(String argExpr, Map<String, List<Object>> evalContext) {
        ELProcessor elp = new ELProcessor();
        ELManager elm = elp.getELManager();
        ExpressionFactory factory = ELManager.getExpressionFactory();
        evalContext.forEach((field, value) -> {
            if (value.size() == 1) {
                elm.setVariable(field, factory.createValueExpression(value.get(0), Object.class));
            } else {
                elm.setVariable(field, factory.createValueExpression(value, Object.class));
            }
        });
        ELContext context = elm.getELContext();
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
