package com.hitsoft.mongo.managed;

import com.sun.jmx.snmp.Enumerated;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Enumeration;

/**
 * Annotation to mark Enum fields for proper values resolve.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface EnumField {
    public Class type();
}
