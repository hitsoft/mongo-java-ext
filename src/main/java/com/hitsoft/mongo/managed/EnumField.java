package com.hitsoft.mongo.managed;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * User: Maxim S. Ivanov
 * Date: 26.12.10
 * Time: 15:47
 */

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface EnumField {
  //    public Class type() default void.class;
  public Class type();
}
