package org.janelia.jacs2.utils;

import javax.inject.Named;

/**
 * Common utility methods for services.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class ServiceUtils {

    public static String getName(Class<?> serviceClass) {
        Named annotation = serviceClass.getAnnotation(Named.class);
        if (annotation==null) {
            throw new AssertionError("Class does not have @Named annotation: "+serviceClass);
        }
        return annotation.value();

    }
}
