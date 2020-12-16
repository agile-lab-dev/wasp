package org.apache.spark.sql.kafka011;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * A ScalaTest tag that indicates a test which is slow, that is takes > 1 minute to run.
 *
 * @author Nicolò Bidotti
 */
@org.scalatest.TagAnnotation
@Retention(RUNTIME)
@Target({METHOD, TYPE})
public @interface Slow { }
