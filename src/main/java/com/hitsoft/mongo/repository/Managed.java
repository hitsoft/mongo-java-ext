package com.hitsoft.mongo.repository;

import com.hitsoft.mongo.basic.*;
import com.hitsoft.mongo.managed.ManagedObject;
import com.hitsoft.mongo.managed.ManagedService;
import com.mongodb.AggregationOutput;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;

import java.util.*;

public class Managed {

  private static Basic getRepository(Class<?> clazz) {
    return Basic.getRepository(ManagedObject.getCollectionName(clazz));
  }

  public static <T extends ManagedObject> T insert(T object) {
    DBObject obj = ManagedService.toDBObject(object);
    Basic repo = getRepository(object.getClass());
    T result = ManagedService.fromDBObject(repo.insert(obj), (Class<T>) object.getClass());
    object._id = result._id;
    return result;
  }

  private static <T extends ManagedObject> void save(T object) {
    getRepository(object.getClass()).save(ManagedService.toDBObject(object));
  }

  public static <T extends ManagedObject> T fetch(Class<T> clazz, ObjectId _id) {
    return byId(clazz, _id)
            .exec
            .fetchOne();
  }

  public static <T extends ManagedObject> QueryBuilder<T> byId(Class<T> clazz, ObjectId _id) {
    return query(clazz)
            .equal(DBObjectExt.Field._ID, _id);
  }

  public static <T extends ManagedObject> QueryBuilder<T> query(Class<T> clazz) {
    return new QueryBuilder(clazz);
  }

  public interface Processor<T extends Object> {
    void process(T object);
  }

  public interface Filter<T extends ManagedObject> {
    boolean accept(T object);
  }

  public static class Aggregate {

    private final Class<ManagedObject> clazz;
    private List<DBObjectExt> obj = new ArrayList<DBObjectExt>();

    private Aggregate(Class<ManagedObject> clazz) {
      this.clazz = clazz;
    }

    public static Aggregate start(Class<ManagedObject> clazz) {
      return new Aggregate(clazz);
    }

    private Aggregate addOperation(Operation operation, Object data) {
      obj.add(DBObjectBuilder.start()
              .add(operation, data)
              .get());
      return this;
    }

    public Aggregate group(Aggregate.Group group) {
      return addOperation(Operation.$GROUP, group.get());
    }

    public Aggregate match(SearchBuilder query) {
      return addOperation(Operation.$MATCH, query.getExt());
    }

    public static class Exec {

      private final AggregationOutput result;

      public Exec(AggregationOutput result) {
        this.result = result;
      }

      public List<DBObjectExt> fetch() {
        List<DBObjectExt> res = new ArrayList<DBObjectExt>();
        Iterator<DBObject> it = result.results().iterator();
        while (it.hasNext()) {
          res.add(new BasicDBObjectExt(it.next()));
        }
        return res;
      }

      public void iterate(Processor<DBObjectExt> processor) {
        Iterator<DBObject> it = result.results().iterator();
        while (it.hasNext()) {
          processor.process(new BasicDBObjectExt(it.next()));
        }
      }

      public DBObjectExt fetchOne() {
        DBObjectExt res;
        Iterator<DBObject> it = result.results().iterator();
        if (it.hasNext()) {
          res = new BasicDBObjectExt(it.next());
          if (it.hasNext()) {
            throw new IllegalStateException("More then one object found");
          }
        } else {
          res = null;
        }
        return res;
      }

      public DBObjectExt fetchFirst() {
        DBObjectExt res;
        Iterator<DBObject> it = result.results().iterator();
        if (it.hasNext()) {
          res = new BasicDBObjectExt(it.next());
        } else {
          res = null;
        }
        return res;
      }
    }

    public Exec exec() {
      DBObject firstObj = obj.get(0).asDBObject();
      DBObject[] additionalOps = new DBObject[obj.size() - 1];
      for (int i = 0; i < additionalOps.length; i++) {
        additionalOps[i] = obj.get(i + 1).asDBObject();
      }
      return new Exec(getRepository(clazz).collection.aggregate(firstObj, additionalOps));
    }

    public Aggregate skip(int skip) {
      return addOperation(Operation.$SKIP, skip);
    }

    public Aggregate limit(int limit) {
      return addOperation(Operation.$LIMIT, limit);
    }

    public enum Operation {
      /**
       * Reshapes a document stream. $project can rename, add, or remove fields as well as create computed values and sub-documents.
       */
      $PROJECT,
      /**
       * Filters the document stream, and only allows matching documents to pass into the next pipeline stage. $match uses standard MongoDB queries.
       */
      $MATCH,
      /**
       * Restricts the number of documents in an aggregation pipeline.
       */
      $LIMIT,
      /**
       * Skips over a specified number of documents from the pipeline and returns the rest.
       */
      $SKIP,
      /**
       * Takes an array of documents and returns them as a stream of documents.
       */
      $UNWIND,
      /**
       * Groups documents together for the purpose of calculating aggregate values based on a collection of documents.
       */
      $GROUP,
      /**
       * Takes all input documents and returns them in a stream of sorted documents.
       */
      $SORT,
      /**
       * Returns an ordered stream of documents based on proximity to a geospatial point.
       */
      $GEO_NEAR
    }

    public static class Group {

      private DBObjectBuilder obj = DBObjectBuilder.start();

      public static Group start() {
        return new Group();
      }

      private static String groupFieldName(Enum field) {
        return String.format("$%s", FieldName.get(field));
      }

      public Group _id(Enum... field) {
        switch (field.length) {
          case 0:
            obj.add(ManagedObject.Field._ID, null);
            break;
          case 1:
            obj.add(ManagedObject.Field._ID, groupFieldName(field[0]));
            break;
          default:
            DBObjectBuilder fields = DBObjectBuilder.start();
            for (Enum f : field) {
              fields.add(f, groupFieldName(f));
            }
            obj.add(ManagedObject.Field._ID, fields.get());
        }
        return this;
      }

      /**
       * суммирует значения поля
       *
       * @param toField итоговое поле в выходном объекте
       * @param field   поле в запрашиваемом объекте по которому будет производиться суммирование
       * @return
       */
      public Group sum(Enum toField, Enum field) {
        obj.add(toField, DBObjectBuilder.start()
                .add(Operation.$SUM, groupFieldName(field))
                .get());
        return this;
      }

      public Group sum(Enum field) {
        return sum(field, field);
      }

      /**
       * считает количество записей попавших под группировку
       *
       * @param toField итоговое поле в выходном объекте
       * @return
       */
      public Group count(Enum toField) {
        obj.add(toField, DBObjectBuilder.start()
                .add(Operation.$SUM, 1)
                .get());
        return this;
      }

      public Group first(Enum toField, Enum field) {
        obj.add(toField, DBObjectBuilder.start()
                .add(Operation.$FIRST, groupFieldName(field))
                .get());
        return this;
      }

      public DBObjectExt get() {
        DBObjectExt result = obj.get();
        if (!result.asDBObject()
                .containsField(FieldName.get(ManagedObject.Field._ID))) {
          result.put(ManagedObject.Field._ID, null);
        }
        return result;
      }

      public static enum Operation {
        $ADD_TO_SET,
        $FIRST,
        $LAST,
        $MAX,
        $MIN,
        $AVG,
        $PUSH,
        $SUM
      }
    }
  }

  public static class QueryBuilder<T extends ManagedObject> {

    // Execute
    public Exec<T> exec = new Exec<T>();
    private PartialObjectBuilder partialBuilder;
    private SearchBuilder searchBuilder = SearchBuilder.start();
    private SortBuilder sortBuilder = SortBuilder.start();
    private int limit = 0;
    private int skip = 0;
    private Class<T> clazz;

    public QueryBuilder(Class<T> clazz) {
      this.clazz = clazz;
    }

    public QueryBuilder<T> prepared(DBObject obj) {
      searchBuilder = SearchBuilder.fromDBObject(obj);
      return this;
    }

    // Limit fields
    public QueryBuilder<T> selectFields(Enum field) {
      if (partialBuilder == null) {
        partialBuilder = new ShowFieldsBuilder();
      }
      assert partialBuilder instanceof ShowFieldsBuilder : "Impossible to use Show fields and Hide fields in the same query.";
      ((ShowFieldsBuilder) partialBuilder).add(field);
      return this;
    }

    public QueryBuilder<T> selectFields(Enum[] fields) {
      for (Enum field : fields)
        selectFields(field);
      return this;
    }

    public QueryBuilder<T> selectIdsOnly() {
      return selectFields(DBObjectExt.Field._ID);
    }

    public QueryBuilder<T> hideFields(Enum field) {
      if (partialBuilder == null) {
        partialBuilder = new HideFieldsBuilder();
      }
      assert partialBuilder instanceof HideFieldsBuilder : "Impossible to use Show fields and Hide fields in the same query.";
      ((HideFieldsBuilder) partialBuilder).add(field);
      return this;
    }

    public QueryBuilder<T> hideFields(Enum[] fields) {
      for (Enum field : fields)
        hideFields(field);
      return this;
    }

    // Where conditions
    public QueryBuilder<T> greaterThan(Enum field, Object value) {
      searchBuilder.greaterThan(field, value);
      return this;
    }

    public QueryBuilder<T> lessThan(Enum field, Object value) {
      searchBuilder.lessThan(field, value);
      return this;
    }

    public QueryBuilder<T> lessOrEqualThan(Enum field, Object value) {
      searchBuilder.lessOrEqualThan(field, value);
      return this;
    }

    public QueryBuilder<T> greaterOrEqualThan(Enum field, Object value) {
      searchBuilder.greaterOrEqualThan(field, value);
      return this;
    }

    public QueryBuilder<T> between(Enum field, Object from, Object to) {
      searchBuilder.between(field, from, to);
      return this;
    }

    public QueryBuilder<T> between(Enum field, Object from, Object to, boolean includeFrom, boolean includeTo) {
      searchBuilder.between(field, from, to, includeFrom, includeTo);
      return this;
    }

    public QueryBuilder<T> equal(Enum[] field, Object value) {
      searchBuilder.equal(field, value);
      return this;
    }

    public QueryBuilder<T> equal(Enum field, Object value) {
      searchBuilder.equal(field, value);
      return this;
    }

    public QueryBuilder<T> notEqual(Enum field, Object value) {
      searchBuilder.notEquals(field, value);
      return this;
    }

    public QueryBuilder<T> notEqual(Enum[] field, Object value) {
      searchBuilder.notEquals(field, value);
      return this;
    }

    public QueryBuilder<T> in(Enum field, Enum[] values) {
      List<String> vals = new ArrayList<String>();
      for (Enum val : values)
        vals.add(val.name());
      return in(field, vals);
    }

    public QueryBuilder<T> in(Enum[] field, Enum[] values) {
      List<String> vals = new ArrayList<String>();
      for (Enum val : values)
        vals.add(val.name());
      searchBuilder.in(field, vals);
      return this;
    }

    public QueryBuilder<T> in(Enum field, Collection<String> values) {
      return in(field, values, false);
    }

    public QueryBuilder<T> in(Enum field, Collection<String> values, boolean asObjectId) {
      if (asObjectId) {
        List<ObjectId> res = new ArrayList<ObjectId>();
        for (String id : values) 
          res.add(new ObjectId(id));
        searchBuilder.in(field, res);
      } else
        searchBuilder.in(field, values);
      return this;
    }

    public QueryBuilder<T> idIn(Collection<String> ids) {
      List<ObjectId> res = new ArrayList<ObjectId>();
      for (String id : ids) {
        res.add(new ObjectId(id));
      }
      searchBuilder.in(DBObjectExt.Field._ID, res);
      return this;
    }

    public QueryBuilder<T> idNotIn(Collection<String> ids) {
      List<ObjectId> res = new ArrayList<ObjectId>();
      for (String id : ids) {
        res.add(new ObjectId(id));
      }
      searchBuilder.notIn(DBObjectExt.Field._ID, res);
      return this;
    }

    public QueryBuilder<T> and(QueryBuilder<T>... builders) {
      List<SearchBuilder> conditions = new ArrayList<SearchBuilder>();
      for (QueryBuilder<T> builder : builders) {
        conditions.add(builder.searchBuilder);
      }
      searchBuilder.and(conditions);
      return this;
    }

    public QueryBuilder<T> or(QueryBuilder<T>... builders) {
      List<SearchBuilder> conditions = new ArrayList<SearchBuilder>();
      for (QueryBuilder<T> builder : builders) {
        conditions.add(builder.searchBuilder);
      }
      searchBuilder.or(conditions);
      return this;
    }

    public QueryBuilder<T> skip(int skip) {
      this.skip = skip;
      return this;
    }

    public QueryBuilder<T> limit(int limit) {
      this.limit = limit;
      return this;
    }

    // Ordering
    public QueryBuilder<T> ascOrderBy(Enum[] field) {
      sortBuilder.asc(field);
      return this;
    }
    public QueryBuilder<T> ascOrderBy(Enum field) {
      return ascOrderBy(new Enum[]{field});
    }

    public QueryBuilder<T> descOrderBy(Enum[] field) {
      sortBuilder.desc(field);
      return this;
    }
    public QueryBuilder<T> descOrderBy(Enum field) {
      return descOrderBy(new Enum[]{field});
    }

    public QueryBuilder<T> orderByAcs(Enum[] fields) {
      for (Enum field : fields)
        sortBuilder.asc(field);
      return this;
    }

    public QueryBuilder<T> orderByDecs(Enum[] fields) {
      for (Enum field : fields)
        sortBuilder.desc(field);
      return this;
    }

    public class Exec<T extends ManagedObject> {
      private DBCursor getCursor() {
        DBObject keys = null;
        if (partialBuilder != null)
          keys = partialBuilder.get();
        DBCursor dbCursor = getRepository(clazz).collection.find(searchBuilder.get(), keys);
        if (!sortBuilder.get().keySet().isEmpty())
          dbCursor.sort(sortBuilder.get());
        if (skip > 0)
          dbCursor = dbCursor.skip(skip);
        if (limit > 0)
          dbCursor = dbCursor.limit(limit);
        return dbCursor;
      }

      public Exec<T> skip(int aSkip) {
        skip = aSkip;
        return this;
      }

      public Exec<T> limit(int aLimit) {
        limit = aLimit;
        return this;
      }

      // Ordering
      public Exec<T> ascOrderBy(Enum[] field) {
        sortBuilder.asc(field);
        return this;
      }
      public Exec<T> ascOrderBy(Enum field) {
        return ascOrderBy(new Enum[]{field});
      }

      public Exec<T> descOrderBy(Enum[] field) {
        sortBuilder.desc(field);
        return this;
      }
      public Exec<T> descOrderBy(Enum field) {
        return descOrderBy(new Enum[]{field});
      }

      public Exec<T> orderByAcs(Enum[] fields) {
        for (Enum field : fields)
          sortBuilder.asc(field);
        return this;
      }

      public Exec<T> orderByDecs(Enum[] fields) {
        for (Enum field : fields)
          sortBuilder.desc(field);
        return this;
      }

      public T fetchOne() {
        DBObjectExt result;
        DBCursor cur = getCursor();
        if (cur.count() == 1) {
          result = new BasicDBObjectExt(cur.next());
        } else if (cur.count() > 1) {
          throw new IllegalStateException("More then one object found");
        } else {
          result = null;
        }
        if (result == null)
          return null;
        else
          return (T) ManagedService.fromDBObject(result.asDBObject(), clazz);
      }

      public T fetchFirst() {
        DBObjectExt result;
        DBCursor cur = getCursor();
        if (cur.count() > 0) {
          result = new BasicDBObjectExt(cur.next());
        } else {
          result = null;
        }
        if (result == null)
          return null;
        else
          return (T) ManagedService.fromDBObject(result.asDBObject(), clazz);
      }

      public List<T> fetch() {
        List<T> result = new ArrayList<T>();
        DBCursor cur = getCursor();
        while (cur.hasNext()) {
          result.add((T) ManagedService.fromDBObject(cur.next(), clazz));
        }
        return result;
      }

      public List fetchDistinct(Enum field) {
        return getRepository(clazz).collection.distinct(FieldName.get(field), searchBuilder.get());
      }

      public List<String> fetchDistinctStrings(Enum field) {
        List<String> result = new ArrayList<String>();
        for (Object obj : fetchDistinct(field)) {
          if (obj != null)
            result.add(obj.toString());
        }
        Collections.sort(result);
        return result;
      }

      public List<Integer> fetchDistinctIntegers(Enum field) {
        List<Integer> result = new ArrayList<Integer>();
        for (Object obj : fetchDistinct(field)) {
          if (Integer.class.isAssignableFrom(obj.getClass())) {
            result.add((Integer) obj);
          }
        }
        Collections.sort(result);
        return result;
      }

      public void iterate(Processor<T> processor) {
        DBCursor cur = getCursor();
        while (cur.hasNext()) {
          processor.process((T) ManagedService.fromDBObject(cur.next(), clazz));
        }
      }

      public void iterateUpdate(Processor<T> processor) {
        DBCursor cur = getCursor();
        while (cur.hasNext()) {
          T obj = (T) ManagedService.fromDBObject(cur.next(), clazz);
          processor.process(obj);
          save(obj);
        }
      }

      public UpdateBuilder update() {
        return new UpdateBuilder(getRepository(clazz), searchBuilder);
      }

      public void remove() {
        getRepository(clazz).collection.remove(searchBuilder.get());
      }

      public long count() {
        return getRepository(clazz).collection.count(searchBuilder.get());
      }

      public Aggregate aggregate() {
        Aggregate result = Aggregate.start((Class<ManagedObject>) clazz);
        result.match(searchBuilder);
        if (skip > 0)
          result.skip(skip);
        if (limit > 0)
          result.limit(limit);
        return result;
      }
    }
  }
}
