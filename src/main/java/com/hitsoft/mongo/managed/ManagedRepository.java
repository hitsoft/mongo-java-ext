package com.hitsoft.mongo.managed;

import com.hitsoft.mongo.basic.*;
import org.bson.types.ObjectId;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * Extending BasicRepository to work with ManagedObjects
 */
public class ManagedRepository {

    public interface Processor<T extends ManagedObject> {
        void process(T object);
    }

    public interface Filter<T extends ManagedObject> {
        boolean accept(T object);
    }

    private static String getCollectionName(Class<?> clazz) {
        return clazz.getSimpleName();
    }

    public static <T extends ManagedObject> T save(T object) {

        return object;
    }

    static <T extends ManagedObject> T loadFromDBObjectExt(Class<T> clazz, DBObjectExt obj) throws IllegalAccessException, InstantiationException {
        T result = clazz.newInstance();
        for (Field field : clazz.getFields()) {
            EnumField enumField = field.getAnnotation(EnumField.class);
            ListField listField = field.getAnnotation(ListField.class);
            if (enumField != null) {
                //noinspection unchecked
                field.set(result, obj.getV(field.getName()).asEnum(enumField.type()));
            } else if (listField != null) {
                List res = new ArrayList<Object>();
                for (Object val : ((List) obj.getV(field.getName()).val())) {
                    if (ManagedObject.class.isAssignableFrom(listField.type())) {
                        //noinspection unchecked
                        res.add(loadFromDBObjectExt((Class<ManagedObject>) listField.type(), (DBObjectExt) val));
                    } else {
                        //noinspection unchecked
                        res.add(val);
                    }
                }
                field.set(result, res);
            } else if (ManagedObject.class.isAssignableFrom(field.getType())) {
                //noinspection unchecked
                field.set(result, loadFromDBObjectExt((Class<ManagedObject>) field.getType(), obj.getV(field.getName()).asObject()));
            } else {
                field.set(result, obj.getV(field.getName()).val());
            }
        }
        return result;
    }

    static DBObjectExt saveToDBObjectExt(ManagedObject obj) throws IllegalAccessException {
        DBObjectExt result;
        if (obj == null) {
            result = null;
        } else {
            result = DBObjectBuilder.start().get();
            for (Field field : obj.getClass().getFields()) {
                EnumField enumField = field.getAnnotation(EnumField.class);
                ListField listField = field.getAnnotation(ListField.class);
                MongoRef mongoRef = field.getAnnotation(MongoRef.class);
                Class fieldType = field.getType();
                String fieldName = field.getName();
                Object fieldVal = field.get(obj);
                if (fieldType.isEnum()) {
                    assert (enumField != null) : String.format("ManagedObject class '%s', enum field '%s' do not have @EnumField(type = %s) annotation specified. Please fix.", obj.getClass().getName(), fieldName, fieldType.getName());
                    result.put(fieldName, ((Enum) fieldVal).name());
                } else if (List.class.isAssignableFrom(fieldType)) {
                    assert (listField != null) : String.format("ManagedObject class '%s', list field '%s' do not have @ListField(type = ???) annotation specified. Please fix.", obj.getClass().getName(), fieldName);
                    List res = new ArrayList<Object>();
                    for (Object val : (List) fieldVal) {
                        if (val instanceof ManagedObject) {
                            //noinspection unchecked
                            res.add(saveToDBObjectExt((ManagedObject) val));
                        } else {
                            //noinspection unchecked
                            res.add(val);
                        }
                    }
                    result.put(fieldName, res);
                } else if (ManagedObject.class.isAssignableFrom(fieldType)) {
                    result.put(fieldName, saveToDBObjectExt((ManagedObject) fieldVal));
                } else if (ObjectId.class.isAssignableFrom(fieldType)) {
                    if (!fieldName.equalsIgnoreCase(FieldName.get(ManagedObject.Field._ID))) {
                        assert (mongoRef != null) : String.format("ManagedObject class '%s', ObjectId field '%s' do not have @MongoRef(ref = ???) annotation specified. Please fix.", obj.getClass().getName(), fieldName);
                    }
                    result.put(fieldName, fieldVal);
                } else {
                    result.put(fieldName, fieldVal);
                }
            }
        }
        return result;
    }

    public static class FindBuilder<T extends ManagedObject> {

        BasicRepository.FindBuilder builder;
        Class<T> clazz;

        FindBuilder(Class<T> clazz) {
            this.clazz = clazz;
            builder = BasicRepository.getRepository(clazz.getSimpleName()).find();
        }

        public T byId(ObjectId id) throws BasicRepository.NotUniqueCondition, InstantiationException, IllegalAccessException {
            builder.where(SearchBuilder.start().equal(ManagedObject.Field._ID, id));
            return loadFromDBObjectExt(clazz, builder.execOne());
        }

        public FindBuilder bySample(ManagedObject sample) {
            return this;
        }
    }

    public static <T extends ManagedObject> FindBuilder<T> find(Class<T> clazz) {
        return new FindBuilder<T>(clazz);
    }

}
