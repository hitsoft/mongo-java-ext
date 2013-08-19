package com.hitsoft.mongo.managed;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by IntelliJ IDEA.
 * User: smeagol
 * Date: 09.07.12
 * Time: 16:56
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ObjectField {
  //    public Class type() default void.class;
  public Class type();
}
