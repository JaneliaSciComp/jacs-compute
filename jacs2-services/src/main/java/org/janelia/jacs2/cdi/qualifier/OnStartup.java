package org.janelia.jacs2.cdi.qualifier;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Beans anotated with this type will be initialized eagerly at startup time if they are also application scoped.
 *
 * Note that this is a @Qualifier annotation so it cannot be used to qualify beans at injection points.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.PARAMETER})
public @interface OnStartup {
}
