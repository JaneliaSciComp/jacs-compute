package org.janelia.model.access.dao.mongo.utils;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.AnnotatedField;
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;
import org.jongo.marshall.jackson.oid.MongoId;
import org.reflections.ReflectionUtils;

public class MongoNamingStrategy extends PropertyNamingStrategy {

    @SuppressWarnings("unchecked")
    @Override
    public String nameForField(MapperConfig<?> config, AnnotatedField field, String defaultName) {
        if (field.getAnnotated().isAnnotationPresent(MongoId.class)) {
            return "_id";
        } else {
            return super.nameForField(config, field, defaultName);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public String nameForGetterMethod(MapperConfig<?> config, AnnotatedMethod method, String defaultName) {
        return ReflectionUtils.getAllFields(method.getDeclaringClass())
                .stream()
                .filter(f -> f.getName().equals(defaultName))
                .filter(f -> f.isAnnotationPresent(MongoId.class))
                .findFirst()
                .map(fname -> "_id")
                .orElseGet(() -> super.nameForGetterMethod(config, method, defaultName));
    }

    @SuppressWarnings("unchecked")
    @Override
    public String nameForSetterMethod(MapperConfig<?> config, AnnotatedMethod method, String defaultName) {
        return ReflectionUtils.getAllFields(method.getDeclaringClass())
                .stream()
                .filter(f -> f.getName().equals(defaultName))
                .filter(f -> f.isAnnotationPresent(MongoId.class))
                .findFirst()
                .map(fname -> "_id")
                .orElseGet(() -> super.nameForSetterMethod(config, method, defaultName));
    }

}
