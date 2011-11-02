package com.hitsoft.mongo.managed;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation to mark special internal field _id
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface MongoId {
}
