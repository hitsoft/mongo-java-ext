package com.hitsoft.mongo.repository;

import com.hitsoft.mongo.basic.*;
import com.hitsoft.mongo.managed.ManagedObject;
import com.hitsoft.mongo.managed.ManagedService;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class Managed {

  public interface Processor<T extends ManagedObject> {
    void process(T object);
  }

  public interface Filter<T extends ManagedObject> {
    boolean accept(T object);
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
      public Exec<T> ascOrderBy(Enum field) {
        sortBuilder.asc(field);
        return this;
      }

      public Exec<T> descOrderBy(Enum field) {
        sortBuilder.desc(field);
        return this;
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

    public QueryBuilder<T> in(Enum field, Enum[] values) {
      List<String> vals = new ArrayList<String>();
      for (Enum val : values)
        vals.add(val.name());
      return in(field, vals);
    }

    public QueryBuilder<T> in(Enum field, Collection<String> values) {
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
    public QueryBuilder<T> ascOrderBy(Enum field) {
      sortBuilder.asc(field);
      return this;
    }

    public QueryBuilder<T> descOrderBy(Enum field) {
      sortBuilder.desc(field);
      return this;
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
  }

  private static Basic getRepository(Class<?> clazz) {
    return Basic.getRepository(ManagedObject.getCollectionName(clazz));
  }

  public static <T extends ManagedObject> T insert(T object) {
    assert (object._id == null) : "Do not support saving of old objects. Please use updates instead.";
    DBObject obj = ManagedService.toDBObject(object);
    Basic repo = Basic.getRepository(object.getCollectionName());
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
}
