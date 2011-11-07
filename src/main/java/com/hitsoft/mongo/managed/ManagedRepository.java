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

    private final Connection connection;

    public ManagedRepository(Connection connection) {
        this.connection = connection;
    }

    public interface Processor<T extends ManagedObject> {
        void process(T object);
    }

    public interface Filter<T extends ManagedObject> {
        boolean accept(T object);
    }

    private BasicRepository getRepository(Class<?> clazz) {
        return BasicRepository.getRepository(connection, clazz.getSimpleName());
    }

    public <T extends ManagedObject> T save(T object) throws IllegalAccessException, InstantiationException {
        DBObjectExt obj = saveToDBObjectExt(object);
        BasicRepository rep = getRepository(object.getClass());
        obj = rep.save(obj);
        loadToDBObjectExt(object, obj);
        return object;
    }

    static <T extends ManagedObject> void loadToDBObjectExt(T object, DBObjectExt from) throws IllegalAccessException, InstantiationException {
        object._id = from.getId();
        Class<T> clazz = (Class<T>) object.getClass();
        for (Field field : clazz.getFields()) {
            EnumField enumField = field.getAnnotation(EnumField.class);
            ListField listField = field.getAnnotation(ListField.class);
            if (enumField != null) {
                //noinspection unchecked
                field.set(object, from.getV(field.getName()).asEnum(enumField.type()));
            } else if (listField != null) {
                List res = new ArrayList<Object>();
                for (Object val : ((List) from.getV(field.getName()).val())) {
                    if (ManagedObject.class.isAssignableFrom(listField.type())) {
                        //noinspection unchecked
                        res.add(loadFromDBObjectExt((Class<ManagedObject>) listField.type(), (DBObjectExt) val));
                    } else {
                        //noinspection unchecked
                        res.add(val);
                    }
                }
                field.set(object, res);
            } else if (ManagedObject.class.isAssignableFrom(field.getType())) {
                //noinspection unchecked
                field.set(object, loadFromDBObjectExt((Class<ManagedObject>) field.getType(), from.getV(field.getName()).asObject()));
            } else {
                field.set(object, from.getV(field.getName()).val());
            }
        }
    }

    static <T extends ManagedObject> T loadFromDBObjectExt(Class<T> clazz, DBObjectExt obj) throws IllegalAccessException, InstantiationException {
        T result = clazz.newInstance();
        loadToDBObjectExt(result, obj);
        return result;
    }

    static void saveToDBObjectExt(ManagedObject managed, DBObjectExt ext) throws IllegalAccessException {
        ext.setId(managed.id());
        for (Field field : managed.getClass().getFields()) {
            EnumField enumField = field.getAnnotation(EnumField.class);
            ListField listField = field.getAnnotation(ListField.class);
            MongoRef mongoRef = field.getAnnotation(MongoRef.class);
            Class fieldType = field.getType();
            String fieldName = field.getName();
            Object fieldVal = field.get(managed);
            if (fieldType.isEnum()) {
                assert (enumField != null) : String.format("ManagedObject class '%s', enum field '%s' do not have @EnumField(type = %s) annotation specified. Please fix.", managed.getClass().getName(), fieldName, fieldType.getName());
                ext.put(fieldName, ((Enum) fieldVal).name());
            } else if (List.class.isAssignableFrom(fieldType)) {
                assert (listField != null) : String.format("ManagedObject class '%s', list field '%s' do not have @ListField(type = ???) annotation specified. Please fix.", managed.getClass().getName(), fieldName);
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
                ext.put(fieldName, res);
            } else if (ManagedObject.class.isAssignableFrom(fieldType)) {
                ext.put(fieldName, saveToDBObjectExt((ManagedObject) fieldVal));
            } else if (ObjectId.class.isAssignableFrom(fieldType)) {
                if (!fieldName.equalsIgnoreCase(FieldName.get(ManagedObject.Field._ID))) {
                    assert (mongoRef != null) : String.format("ManagedObject class '%s', ObjectId field '%s' do not have @MongoRef(ref = ???) annotation specified. Please fix.", managed.getClass().getName(), fieldName);
                }
                ext.put(fieldName, fieldVal);
            } else {
                ext.put(fieldName, fieldVal);
            }
        }
    }

    static DBObjectExt saveToDBObjectExt(ManagedObject obj) throws IllegalAccessException {
        DBObjectExt result;
        if (obj == null) {
            result = null;
        } else {
            result = DBObjectBuilder.start().get();
            saveToDBObjectExt(obj, result);
        }
        return result;
    }

    public class FindBuilder<T extends ManagedObject> {

        BasicRepository.FindBuilder builder;
        Class<T> clazz;

        FindBuilder(Class<T> clazz) {
            this.clazz = clazz;
            builder = getRepository(clazz).find();
        }

        public T byId(ObjectId id) throws BasicRepository.NotUniqueCondition, InstantiationException, IllegalAccessException {
            builder.where(SearchBuilder.start().equal(ManagedObject.Field._ID, id));
            return loadFromDBObjectExt(clazz, builder.execOne());
        }

        public FindBuilder<T> bySample(ManagedObject sample) throws IllegalAccessException {
            DBObjectExt obj = saveToDBObjectExt(sample);
            obj.put(ManagedObject.Field._ID, null);
            builder.where(SearchBuilder.start().equal(obj));
            return this;
        }

        public FindBuilder<T> where(SearchBuilder conditions) {
            builder.where(conditions);
            return this;
        }

        public FindBuilder<T> orderBy(SortBuilder sort) {
            builder.orderBy(sort);
            return this;
        }

        public FindBuilder<T> orderBy(Enum[] fields) {
            builder.orderBy(fields);
            return this;
        }

        public FindBuilder<T> limit(int limit) {
            builder.limit(limit);
            return this;
        }

        public FindBuilder<T> skip(int skip) {
            builder.skip(skip);
            return this;
        }

        public List<T> exec() throws InstantiationException, IllegalAccessException {
            List<T> result = new ArrayList<T>();
            for (DBObjectExt obj : builder.exec()) {
                result.add(loadFromDBObjectExt(clazz, obj));
            }
            return result;
        }

        public T execOne() throws BasicRepository.NotUniqueCondition, InstantiationException, IllegalAccessException {
            T result = null;
            DBObjectExt res = builder.execOne();
            if (res != null)
                result = loadFromDBObjectExt(clazz, res);
            return result;
        }

        public T execFirst() throws InstantiationException, IllegalAccessException {
            T result = null;
            DBObjectExt res = builder.execFirst();
            if (res != null)
                result = loadFromDBObjectExt(clazz, res);
            return result;
        }

        public void execIterate(final Processor processor) throws IllegalAccessException, InstantiationException {
            builder.execIterate(new BasicRepository.Processor() {
                @Override
                public void process(DBObjectExt obj) throws IllegalAccessException, InstantiationException {
                    processor.process(loadFromDBObjectExt(clazz, obj));
                }
            });
        }

        public void execIterateUpdate(final Processor processor) throws IllegalAccessException, InstantiationException {
            builder.execIterateUpdate(new BasicRepository.Processor() {
                @Override
                public void process(DBObjectExt obj) throws IllegalAccessException, InstantiationException {
                    T managed = loadFromDBObjectExt(clazz, obj);
                    processor.process(managed);
                    saveToDBObjectExt(managed, obj);
                }
            });
        }

        public List<T> execFilter(final Filter filter) throws InstantiationException, IllegalAccessException {
            List<T> result = new ArrayList<T>();
            List<DBObjectExt> res = builder.execFilter(new BasicRepository.Filter() {
                @Override
                public boolean accept(DBObjectExt obj) throws IllegalAccessException, InstantiationException {
                    return filter.accept(loadFromDBObjectExt(clazz, obj));
                }
            });
            for (DBObjectExt obj : res) {
                result.add(loadFromDBObjectExt(clazz, obj));
            }
            return result;
        }
    }

    public <T extends ManagedObject> FindBuilder<T> find(Class<T> clazz) {
        return new FindBuilder<T>(clazz);
    }

    public <T extends ManagedObject> T findById(Class<T> clazz, ObjectId id) throws BasicRepository.NotUniqueCondition, IllegalAccessException, InstantiationException {
        return find(clazz)
                .where(SearchBuilder.start().equal(DBObjectExt.Field._ID, id))
                .execOne();
    }

    public <T extends ManagedObject> long count(Class<T> clazz, SearchBuilder conditions) {
        return getRepository(clazz).count(conditions);
    }

    public <T extends ManagedObject> void ensureIndex(Class<T> clazz, SortBuilder orderBy) {
        getRepository(clazz).ensureIndex(orderBy);
    }

    public <T extends ManagedObject> void update(Class<T> clazz, SearchBuilder conditions, UpdateBuilder updates) {
        getRepository(clazz).update(conditions, updates);
    }

    public <T extends ManagedObject> List distinct(Class<T> clazz, Enum field, SearchBuilder conditions) {
        return getRepository(clazz).distinct(field, conditions);
    }

    public <T extends ManagedObject> List<String> distinctStrings(Class<T> clazz, Enum field, SearchBuilder conditions) {
        return getRepository(clazz).distinctStrings(field, conditions);
    }

}
