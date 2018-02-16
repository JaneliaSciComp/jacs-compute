package org.janelia.jacs2.auth.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Specifies that the annotated method or class requires an authenticated subject.
 */
@Documented
@Retention(RUNTIME)
@Target({TYPE, METHOD})
public @interface RequireAuthentication {
}
