package com.hitsoft.mongo.basic;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

/**
 * Mongo repository to work with extended DBObjects (DBObjectExt)
 */
public class BasicRepository {

    /**
     * Interface used for processing database selection with BasicRepository.iterate functions
     */
    public interface Processor {
        void process(DBObjectExt obj) throws IllegalAccessException, InstantiationException;
    }

    /**
     * Interface used to filter our some search result records by programmable conditions
     */
    public interface Filter {
        boolean accept(DBObjectExt obj) throws IllegalAccessException, InstantiationException;
    }

    public class FindBuilder {

        private SearchBuilder conditions;
        private PartialObjectBuilder fields;
        private SortBuilder sort;
        private Integer limit;
        private Integer skip;

        public FindBuilder where(SearchBuilder conditions) {
            this.conditions = conditions;
            return this;
        }

        private FindBuilder partial(PartialObjectBuilder columns) {
            this.fields = columns;
            return this;
        }

        public FindBuilder fieldsShow(Enum[] fields) {
            ShowFieldsBuilder res = ShowFieldsBuilder.start();
            for (Enum field : fields)
                res.add(field);
            return partial(res);
        }

        public FindBuilder fieldsHide(Enum[] fields) {
            HideFieldsBuilder res = HideFieldsBuilder.start();
            for (Enum field : fields)
                res.add(field);
            return partial(res);
        }

        public FindBuilder idsOnly() {
            return fieldsShow(new Enum[]{BasicDBObjectExt.Field._ID});
        }

        public FindBuilder orderBy(SortBuilder sort) {
            this.sort = sort;
            return this;
        }

        public FindBuilder orderBy(Enum[] fields) {
            SortBuilder sort = SortBuilder.start();
            for (Enum field : fields) {
                sort.asc(field);
            }
            return this.orderBy(sort);
        }

        public FindBuilder limit(int limit) {
            this.limit = limit;
            return this;
        }

        public FindBuilder skip(int skip) {
            this.skip = skip;
            return this;
        }

        DBCursor doExec() {
            if (conditions == null)
                conditions = SearchBuilder.start();
            DBObject flds = null;
            if (fields != null)
                flds = fields.get();
            DBCursor result = collection.find(conditions.get(), flds);
            if (sort != null && sort.get().toMap().size() > 0)
                result.sort(sort.get());
            if (skip != null)
                result.skip(skip);
            if (limit != null)
                result.limit(limit);
            return result;
        }

        public List<DBObjectExt> exec() {
            List<DBObjectExt> result = new ArrayList<DBObjectExt>();
            DBCursor cursor = doExec();
            while (cursor.hasNext()) {
                DBObjectExt obj = new BasicDBObjectExt(cursor.next());
                result.add(obj);
            }
            return result;
        }

        public DBObjectExt execOne() throws NotUniqueCondition {
            DBObjectExt result = null;
            DBCursor cursor = doExec();
            if (cursor.count() == 1) {
                result = new BasicDBObjectExt(cursor.next());
            } else if (cursor.count() > 1) {
                throw new NotUniqueCondition(conditions);
            }
            return result;
        }

        public DBObjectExt execFirst() {
            DBObjectExt result = null;
            DBCursor cursor = doExec();
            if (cursor.hasNext())
                result = new BasicDBObjectExt(cursor.next());
            return result;
        }

        public void execIterate(Processor processor) throws InstantiationException, IllegalAccessException {
            DBCursor cursor = doExec();
            while (cursor.hasNext()) {
                DBObjectExt obj = new BasicDBObjectExt(cursor.next());
                processor.process(obj);
            }
        }

        public void execIterateUpdate(Processor processor) throws InstantiationException, IllegalAccessException {
            DBCursor cursor = doExec();
            while (cursor.hasNext()) {
                DBObjectExt obj = new BasicDBObjectExt(cursor.next());
                processor.process(obj);
                collection.save(obj.asDBObject());
            }
        }

        public List<DBObjectExt> execFilter(Filter filter) throws InstantiationException, IllegalAccessException {
            List<DBObjectExt> result = new ArrayList<DBObjectExt>();
            DBCursor cursor = doExec();
            while (cursor.hasNext()) {
                DBObjectExt obj = new BasicDBObjectExt(cursor.next());
                if (filter.accept(obj))
                    result.add(obj);
            }
            return result;
        }

    }

    private DBCollection collection;

    private BasicRepository(Connection connection, String name) {
        this.collection = connection.getCollection(name);
    }

    public static BasicRepository getRepository(Connection connection, String collectionName) {
        return new BasicRepository(connection, collectionName);
    }

    public DBObjectExt save(DBObjectExt obj) {
        collection.save(obj.asDBObject());
        return obj;
    }

    public void remove(DBObjectExt obj) {
        collection.remove(obj.asDBObject());
    }

    public FindBuilder find() {
        return new FindBuilder();
    }

    public DBObjectExt findById(ObjectId id) throws NotUniqueCondition {
        return find()
                .where(SearchBuilder.start().equal(DBObjectExt.Field._ID, id))
                .execOne();
    }

    public long count(SearchBuilder conditions) {
        return collection.count(conditions.get());
    }

    public void ensureIndex(SortBuilder orderBy) {
        collection.ensureIndex(orderBy.get());
    }

    public void update(SearchBuilder conditions, UpdateBuilder updates) {
        collection.update(conditions.get(), updates.get(), false, true);
    }

    public List distinct(Enum field, SearchBuilder conditions) {
        return collection.distinct(FieldName.get(field), conditions.get());
    }

    public List<String> distinctStrings(Enum field, SearchBuilder conditions) {
        List<String> result = new ArrayList<String>();
        for (Object val : distinct(field, conditions)) {
            result.add(val.toString());
        }
        return result;
    }

    public class NotUniqueCondition extends Throwable {
        public NotUniqueCondition(SearchBuilder conditions) {
            super(String.format("Not unique conditions: '%s'", conditions.get().toString()));
        }
    }
}
