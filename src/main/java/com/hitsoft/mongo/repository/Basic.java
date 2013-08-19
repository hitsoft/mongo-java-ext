package com.hitsoft.mongo.repository;

import com.hitsoft.mongo.basic.*;
import com.mongodb.*;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.*;

public class Basic {

  public DBCollection collection;
  private static Mongo server = null;
  private static DB database = null;
  private static final Logger LOG = LoggerFactory.getLogger(Basic.class);
  private static ConfigDB dbConfig = null;
  public static final String _ID = "_id";

  public static void closeConnection() {
    if (server != null)
      server.close();
  }

  public static class ConfigDB {
    public final String host;
    public final int port;
    public String name;
    // Настройки драйвера Mongo
    public boolean autoConnectRetry = true;
    public int connectionsPerHost = 100;
    public int connectTimeout = 30000;
    public int socketTimeout = 60000;
    public int threadsAllowedToBlockForConnectionMultiplier = 1500;

    public ConfigDB(String host, int port) {
      this.host = host;
      this.port = port;
    }
  }

  public static class DataAccessException extends RuntimeException {
    public DataAccessException(String message, Throwable cause) {
      super(message, cause);
    }
  }


  public static void setDBConfig(ConfigDB config) {
    dbConfig = config;
  }

  public static Basic getRepository(ConfigDB config, String collectionName) {
    try {
      return new Basic(config, collectionName);
    } catch (UnknownHostException e) {
      throw new DataAccessException("Problem on instantiation Mongo Repository", e);
    }
  }

  public static Basic getRepository(String collectionName) {
    return getRepository(dbConfig, collectionName);
  }

  private Basic(ConfigDB config, String collectionName) throws UnknownHostException {
    this.collection = resolveCollection(config, collectionName);
  }

  /**
   * method for storing new object to database.
   * if object have not _ID field it will be added and _ID fromn object will be used otherwise
   *
   * @param object object for storing in DB
   * @return _ID of inserted object
   */
  public DBObject insert(DBObject object) {
    BasicDBObject obj = new BasicDBObject(object.toMap());
    collection.insert(obj);
    return obj;
  }

  public static BasicDBObject buildDBObject(Map<String, Object> properties) {
    BasicDBObject obj = new BasicDBObject();
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      obj = obj.append(entry.getKey(), entry.getValue());
    }
    return obj;
  }

  public void dropRepository() {
    collection.drop();
  }

  private DBCollection resolveCollection(ConfigDB config, String collectionName) throws UnknownHostException {
    if (server == null) {
      server = createServer(config);
      database = server.getDB(config.name);
    }
    return database.getCollection(collectionName);
  }

  private static Mongo createServer(ConfigDB config) throws UnknownHostException {
    MongoOptions options = new MongoOptions();
    options.autoConnectRetry = config.autoConnectRetry;
    options.connectionsPerHost = config.connectionsPerHost;
    options.connectTimeout = config.connectTimeout;
    options.socketTimeout = config.socketTimeout;
    options.threadsAllowedToBlockForConnectionMultiplier = config.threadsAllowedToBlockForConnectionMultiplier;
    return new Mongo(new ServerAddress(config.host, config.port), options);
  }

  public static void dropDatabase(ConfigDB config) throws UnknownHostException {
    Mongo server = createServer(config);
    DB database = server.getDB(config.name);
    database.dropDatabase();
    LOG.info("Database {} dropped", config.name);
  }

  public void ensureIndex(DBObject keys) {
    collection.ensureIndex(keys);
  }

  public void ensureIndexUnique(DBObject keys, boolean dropDuplicates) {
    collection.ensureIndex(keys, DBObjectBuilder.start()
            .add("unique", true)
            .add("dropDups", dropDuplicates)
            .get().asDBObject());
  }

  public void save(DBObject object) {
    collection.save(object);
  }

  public interface Processor {
    void process(DBObjectExt obj);
  }

  public class QueryBuilder {

    private PartialObjectBuilder partialBuilder;
    private SearchBuilder searchBuilder = new SearchBuilder();
    private SortBuilder sortBuilder = new SortBuilder();
    private int limit = 0;
    private int skip = 0;


    // Limit fields
    public QueryBuilder selectFields(Enum field) {
      if (partialBuilder == null) {
        partialBuilder = new ShowFieldsBuilder();
      }
      assert partialBuilder instanceof ShowFieldsBuilder : "Impossible to use Show fields and Hide fields in the same query.";
      ((ShowFieldsBuilder) partialBuilder).add(field);
      return this;
    }

    public QueryBuilder selectFields(Enum[] fields) {
      for (Enum field : fields)
        selectFields(field);
      return this;
    }

    public QueryBuilder selectIdsOnly() {
      return selectFields(DBObjectExt.Field._ID);
    }

    public QueryBuilder hideFields(Enum field) {
      if (partialBuilder == null) {
        partialBuilder = new HideFieldsBuilder();
      }
      assert partialBuilder instanceof HideFieldsBuilder : "Impossible to use Show fields and Hide fields in the same query.";
      ((HideFieldsBuilder) partialBuilder).add(field);
      return this;
    }

    public QueryBuilder hideFields(Enum[] fields) {
      for (Enum field : fields)
        hideFields(field);
      return this;
    }

    // Where conditions
    public QueryBuilder greaterThan(Enum field, Object value) {
      searchBuilder.greaterThan(field, value);
      return this;
    }

    public QueryBuilder lessThan(Enum field, Object value) {
      searchBuilder.lessThan(field, value);
      return this;
    }

    public QueryBuilder lessOrEqualThan(Enum field, Object value) {
      searchBuilder.lessOrEqualThan(field, value);
      return this;
    }

    public QueryBuilder greaterOrEqualThan(Enum field, Object value) {
      searchBuilder.greaterOrEqualThan(field, value);
      return this;
    }

    public QueryBuilder equal(Enum field, Object value) {
      searchBuilder.equal(field, value);
      return this;
    }

    public QueryBuilder notEqual(Enum field, Object value) {
      searchBuilder.notEquals(field, value);
      return this;
    }

    public QueryBuilder in(Enum field, List<String> values) {
      searchBuilder.in(field, values);
      return this;
    }

    public QueryBuilder idIn(Collection<String> ids) {
      List<ObjectId> res = new ArrayList<ObjectId>();
      for (String id : ids) {
        res.add(new ObjectId(id));
      }
      searchBuilder.in(DBObjectExt.Field._ID, res);
      return this;
    }

    public QueryBuilder idNotIn(Collection<String> ids) {
      List<ObjectId> res = new ArrayList<ObjectId>();
      for (String id : ids) {
        res.add(new ObjectId(id));
      }
      searchBuilder.notIn(DBObjectExt.Field._ID, res);
      return this;
    }

    public QueryBuilder skip(int skip) {
      this.skip = skip;
      return this;
    }

    public QueryBuilder limit(int limit) {
      this.limit = limit;
      return this;
    }

    // Ordering
    public QueryBuilder ascOrderBy(Enum field) {
      sortBuilder.asc(field);
      return this;
    }

    public QueryBuilder descOrderBy(Enum field) {
      sortBuilder.desc(field);
      return this;
    }

    public QueryBuilder orderByAcs(Enum[] fields) {
      for (Enum field : fields)
        sortBuilder.asc(field);
      return this;
    }

    public QueryBuilder orderByDecs(Enum[] fields) {
      for (Enum field : fields)
        sortBuilder.desc(field);
      return this;
    }

    // Execute
    public Exec _ = new Exec();

    public class Exec {

      private DBCursor getCursor() {
        DBObject keys = null;
        if (partialBuilder != null)
          keys = partialBuilder.get();
        DBCursor dbCursor = collection.find(searchBuilder.get(), keys);
        if (!sortBuilder.get().keySet().isEmpty())
          dbCursor.sort(sortBuilder.get());
        if (skip > 0)
          dbCursor = dbCursor.skip(skip);
        if (limit > 0)
          dbCursor = dbCursor.limit(limit);
        return dbCursor;
      }

      public DBObjectExt fetchOne() {
        DBObjectExt result;
        DBCursor cur = getCursor();
        if (cur.count() == 1) {
          result = new BasicDBObjectExt(cur.next());
        } else if (cur.count() > 1) {
          throw new IllegalStateException("More then one object found");
        } else {
          result = null;
        }
        return result;
      }

      public DBObjectExt fetchFirst() {
        DBObjectExt result;
        DBCursor cur = getCursor();
        if (cur.count() > 0) {
          result = new BasicDBObjectExt(cur.next());
        } else {
          result = null;
        }
        return result;
      }

      public List<DBObjectExt> fetch() {
        List<DBObjectExt> result = new ArrayList<DBObjectExt>();
        DBCursor cur = getCursor();
        while (cur.hasNext()) {
          result.add(new BasicDBObjectExt(cur.next()));
        }
        return result;
      }

      public List fetchDistinct(Enum field) {
        DBObject obj = null;
        if (searchBuilder != null)
          obj = searchBuilder.get();
        return collection.distinct(FieldName.get(field), obj);
      }

      public List<String> fetchDistinctStrings(Enum field) {
        List<String> result = new ArrayList<String>();
        for (Object obj : fetchDistinct(field))
          result.add(obj.toString());
        Collections.sort(result);
        return result;
      }

      public void iterate(Processor processor) {
        DBCursor cur = getCursor();
        while (cur.hasNext()) {
          processor.process(new BasicDBObjectExt(cur.next()));
        }
      }

      public void iterateUpdate(Processor processor) {
        DBCursor cur = getCursor();
        while (cur.hasNext()) {
          DBObjectExt obj = new BasicDBObjectExt(cur.next());
          processor.process(obj);
          save(obj.asDBObject());
        }
      }

      public UpdateBuilder update() {
        return new UpdateBuilder(Basic.this, searchBuilder);
      }

      public long count() {
        return collection.count(searchBuilder.get());
      }

      public void remove() {
        collection.remove(searchBuilder.get());
      }
    }
  }

  public DBObjectExt fetch(String id) {
    return byId(id)
            ._
            .fetchOne();
  }

  public QueryBuilder byId(String id) {
    return query()
            .equal(DBObjectExt.Field._ID, new ObjectId(id));
  }

  public QueryBuilder query() {
    return new QueryBuilder();
  }
}
