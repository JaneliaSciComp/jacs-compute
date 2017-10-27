package org.janelia.model.jacs2.domain.support;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for marking a field or a getter to indicate that it returns a searchable attribute value. 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface SearchAttribute {
    String key();
    String label();
    String facet() default "";
    boolean display() default true;
}
