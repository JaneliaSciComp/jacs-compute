package org.janelia.jacs2.asyncservice.sample;

import java.lang.annotation.*;

/**
 * Annotation for marking services classes with named/typed parameters.
 * 
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Repeatable(ServiceInputs.class)
public @interface ServiceInput {
    String name();
    Class<?> type();
    String description();
    boolean variadic() default false;
}
