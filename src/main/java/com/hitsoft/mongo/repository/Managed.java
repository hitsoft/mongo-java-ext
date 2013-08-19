package com.hitsoft.mongo.repository;

import com.hitsoft.mongo.basic.*;
import com.hitsoft.mongo.managed.ManagedObject;
import com.hitsoft.mongo.managed.ManagedRepository;
import org.bson.types.ObjectId;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: smeagol
 * Date: 07.11.11
 * Time: 16:49
 */
public class Managed {

    private static Connection connection;

    private static Connection getConnection() {
        if (connection != null)
            return connection;
        else
            return Connection.getInstance();
    }

    private static ManagedRepository rep() {
        return new ManagedRepository(getConnection());
    }

    public static <T extends ManagedObject> T save(T obj) throws InstantiationException, IllegalAccessException {
        return rep().save(obj);
    }

    public static <T extends ManagedObject> ManagedRepository.FindBuilder<T> find(Class<T> clazz) {
        return rep().find(clazz);
    }

    public <T extends ManagedObject> T findById(Class<T> clazz, ObjectId id) throws BasicRepository.NotUniqueCondition, IllegalAccessException, InstantiationException {
        return rep().findById(clazz, id);
    }

    public static <T extends ManagedObject> long count(Class<T> clazz, SearchBuilder conditions) {
        return rep().count(clazz, conditions);
    }

    public static <T extends ManagedObject> void ensureIndex(Class<T> clazz, SortBuilder orderBy) {
        rep().ensureIndex(clazz, orderBy);
    }

    public static <T extends ManagedObject> void update(Class<T> clazz, SearchBuilder conditions, UpdateBuilder updates) {
        rep().update(clazz, conditions, updates);
    }

    public static <T extends ManagedObject> List distinct(Class<T> clazz, Enum field, SearchBuilder conditions) {
        return rep().distinct(clazz, field, conditions);
    }

    public static <T extends ManagedObject> List<String> distinctStrings(Class<T> clazz, Enum field, SearchBuilder conditions) {
        return rep().distinctStrings(clazz, field, conditions);
    }

    public static <T extends ManagedObject> void remove(Class<T> clazz, SearchBuilder conditions) {
        rep().remove(clazz, conditions);
    }

    public static <T extends ManagedObject> void remove(T object) {
        rep().remove(object.getClass(), SearchBuilder.start().equal(ManagedObject.Field._ID, object.id()));
    }
}
