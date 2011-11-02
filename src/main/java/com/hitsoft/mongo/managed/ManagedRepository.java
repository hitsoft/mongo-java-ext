package com.hitsoft.mongo.managed;

import com.hitsoft.mongo.managed.ManagedObject;

/**
 * Extending BasicRepository to work with ManagedObjects
 */
public class ManagedRepository {

    private static String getCollectionName(Class<?> clazz) {
        return clazz.getSimpleName();
    }

    public static <T extends ManagedObject> T fetch(Class<T> clazz, String _id) {
        return null;
    }

    public static <T extends ManagedObject> T save(T obj) {
        return obj;
    }

    public static interface Processor<T extends ManagedObject> {
        void process(T object);
    }

//    public static <T extends ManagedObject> void iterate(Class<T> clazz, QueryBuilder qb, SortBuilder sb, boolean shouldSave, Processor<T> processor) {
//
//    }

}
