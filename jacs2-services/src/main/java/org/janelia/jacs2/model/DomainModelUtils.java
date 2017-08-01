package org.janelia.jacs2.model;

import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang3.StringUtils;
import org.janelia.it.jacs.model.domain.DomainObject;
import org.janelia.it.jacs.model.domain.Subject;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.it.jacs.model.domain.interfaces.HasFiles;
import org.janelia.it.jacs.model.domain.support.MongoMapping;
import org.reflections.ReflectionUtils;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DomainModelUtils {
    private static LoadingCache<String, Class<? extends BaseEntity>> ENTITY_CLASS_CACHE = CacheBuilder.newBuilder()
            .maximumSize(1024)
            .build(new CacheLoader<String, Class<? extends BaseEntity>>() {
                @Override
                public Class<? extends BaseEntity> load(String entityType) throws Exception {
                    return findEntityClass(entityType);
                }
            });

    /**
     * @param subjectKey
     * @return the subject name part of a given subject key. For example, for "group:flylight", this returns "flylight".
     */
    public static String getNameFromSubjectKey(String subjectKey) {
        if (StringUtils.isBlank(subjectKey)) {
            return null;
        }
        List<String> subjectKeyComponents = getSubjectKeyComponents(subjectKey);
        return subjectKeyComponents.get(1);
    }

    /**
     * @param subjectKey
     * @return the type part of the given subject key. For example, for "group:flylight", this returns "group".
     */
    public static String getTypeFromSubjectKey(String subjectKey) {
        if (StringUtils.isBlank(subjectKey)) {
            return null;
        }
        List<String> subjectKeyComponents = getSubjectKeyComponents(subjectKey);
        return subjectKeyComponents.get(0);
    }

    private static List<String> getSubjectKeyComponents(String subjectKey) {
        List<String> subjectKeyComponents = Splitter.on(':').trimResults().splitToList(subjectKey);
        if (subjectKeyComponents.size() != 2) {
            throw new IllegalArgumentException("Invalid subject key '" + subjectKey + "' - expected format <type>:<name>");
        }
        return subjectKeyComponents;
    }

    public static boolean isAdminOrUndefined(Subject subject) {
        return subject == null || subject.isAdmin();
    }

    public static boolean isNotAdmin(Subject subject) {
        return subject != null && !subject.isAdmin();
    }

    @SuppressWarnings("unchecked")
    public static <T> Class<T> getGenericParameterType(Class<?> parameterizedClass, int paramIndex) {
        return (Class<T>)((ParameterizedType)parameterizedClass.getGenericSuperclass()).getActualTypeArguments()[paramIndex];
    }

    public static Class<? extends BaseEntity> getEntityClass(String entityType) {
        return ENTITY_CLASS_CACHE.getUnchecked(entityType);
    }

    private static Class<? extends BaseEntity> findEntityClass(String entityType) {
        Reflections entityReflections = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage("org.janelia"))
                .filterInputsBy(new FilterBuilder().includePackage(BaseEntity.class).includePackage(DomainObject.class)));
        for (Class<? extends BaseEntity> clazz : entityReflections.getSubTypesOf(BaseEntity.class)) {
            if (clazz.getSimpleName().equals(entityType) || clazz.getName().equals(entityType)) {
                return clazz;
            }
        }
        throw new IllegalArgumentException("Unsupported or unknown entityType: " + entityType);
    }

    public static Class<?> getBasePersistedEntityClass(String entityType) {
        Class<?> entityClass = getEntityClass(entityType);
        for(Class<?> clazz = entityClass; clazz != null; clazz = clazz.getSuperclass()) {
            if (clazz.isAnnotationPresent(MongoMapping.class)) {
                return clazz; // first class encountered going up the hierarchy that has a MongoMapping annotation
            }
        }
        // if no annotated class was found assume the current class is the one being persisted.
        return entityClass;
    }

    public static MongoMapping getMapping(Class<?> objectClass) {
        MongoMapping mongoMapping = null;
        for(Class<?> clazz = objectClass; clazz != null; clazz = clazz.getSuperclass()) {
            if (clazz.isAnnotationPresent(MongoMapping.class)) {
                mongoMapping = clazz.getAnnotation(MongoMapping.class);
                break;
            }
        }
        return mongoMapping;
    }

    public static Map<String, Object> setFullPathForFileType(HasFiles objWithFiles, FileType fileType, String fileName) {
        if (StringUtils.isBlank(fileName)) {
            return objWithFiles.removeFileName(fileType);
        } else {
            return objWithFiles.setFileName(fileType, fileName);
        }
    }

    public static <D extends DomainObject> Map<String, Object> getFieldValues(D dObj) {
        try {
            Map<String, Object> objectFields = new HashMap<>();
            for (Field field : ReflectionUtils.getAllFields(dObj.getClass())) {
                field.setAccessible(true);
                objectFields.put(field.getName(), field.get(dObj));
            }
            return objectFields;
        } catch (IllegalAccessException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    /**
     * Given a variable naming pattern, replace the variables with values from the given map. The pattern syntax is as follows:
     * {Variable Name} - Variable by name
     * {Variable Name|Fallback} - Variable, with a fallback value
     * {Variable Name|Fallback|"Value"} - Multiple fallback with static value
     * @param pattern
     * @param valuesContext
     * @return processed output string
     */
    public static String replaceVariables(String pattern, Map<String, Object> valuesContext) {
        Pattern regexPattern = Pattern.compile("\\{(.+?)\\}");
        Matcher matcher = regexPattern.matcher(pattern);
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            String template = matcher.group(1);
            String replacement = null;
            for (String templatePart : template.split("\\|")) {
                String attrLabel = templatePart.trim();
                if (attrLabel.matches("\"(.*?)\"")) {
                    replacement = attrLabel.substring(1, attrLabel.length()-1);
                } else {
                    Object value = valuesContext.get(attrLabel);
                    replacement = value == null ? null : value.toString();
                }
                if (replacement != null) {
                    matcher.appendReplacement(buffer, replacement);
                    break;
                }
            }
            if (replacement == null) {
                matcher.appendReplacement(buffer, ""); // no valid value was found
            }
        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }
}
