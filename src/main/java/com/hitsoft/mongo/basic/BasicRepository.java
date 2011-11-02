package com.hitsoft.mongo.basic;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;

import java.net.UnknownHostException;
import java.util.*;

/**
 * Mongo repository to work with extended DBObjects (DBObjectExt)
 */
public class BasicRepository {
//    private DBCollection collection;
//    private static String DATABASE_PROPERTIES_FILENAME = "/configuration.xml";
//    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedRepository.class);
//    private static final Logger SLOW_LOG = LoggerFactory.getLogger("mobi.poisk.SlowQuery");
//    private static final int SLOW_TIME = 100; // ����������������� "����������" ������� � ������������.
//    private static Config dbConfig = null;
//    public static final String ID = "_id";
//
//    public static void setDBConfig(Config config) {
//        dbConfig = config;
//    }
//
//    public static void setDBConfigPath(String filePath) {
//        DATABASE_PROPERTIES_FILENAME = filePath;
//    }
//
//    public static ManagedRepository getRepository(Config config, String collectionName) {
//        try {
//            return new ManagedRepository(config, collectionName);
//        } catch (UnknownHostException e) {
//            throw new DataAccessException("Problem on instantiation Mongo Repository", e);
//        }
//    }
//
//    public static ManagedRepository getRepository(String collectionName) {
//        return getRepository(getConfig(), collectionName);
//    }
//
//    private static Config getConfig() {
//        if (dbConfig == null)
//            dbConfig = Config.getConfig(DATABASE_PROPERTIES_FILENAME);
//        return dbConfig;
//    }
//
//    private ManagedRepository(Config config, String collectionName) throws UnknownHostException {
//        this.collection = resolveCollection(config, collectionName);
//    }
//
//    /**
//     * method for storing new object to database.
//     * if object have not ID field it will be added and ID fromn object will be used otherwise
//     *
//     * @param object object for storing in DB
//     * @return ID of inserted object
//     */
//    public DBObject insert(DBObject object) {
//        BasicDBObject obj = new BasicDBObject(object.toMap());
//        collection.insert(obj);
//        return obj;
//    }
//
//    public void remove(ObjectId id) {
//        collection.remove(new BasicDBObject(ID, id));
//    }
//
//    public void remove(String id) {
//        remove(new ObjectId(id));
//    }
//
//    public void remove(DBObject obj) {
//        collection.remove(obj);
//    }
//
//    public List<DBObject> findAll() {
//        return toList(collection.find());
//    }
//
//    public void iterateAll(IDBObjectProcessor objectProcessor, boolean save) {
//        iterateAll(objectProcessor, save, null);
//    }
//
//    public void iterateAll(IDBObjectProcessor objectProcessor, boolean save, DBObject orderBy) {
//        DBCursor cursor = collection.find();
//        if (orderBy != null)
//            cursor.sort(orderBy);
//        while (cursor.hasNext()) {
//            DBObject obj = cursor.next();
//            objectProcessor.process(obj);
//            if (save)
//                collection.save(obj);
//        }
//        LOGGER.info("IterateAll - collection full name - " + collection.getFullName() + " count - " + cursor.count());
//    }
//
//    /**
//     * ������� �������� � ���� ������ �������� ��� ���� ��������� ��������
//     *
//     * @param filter          ������ �� ������� ������
//     * @param objectProcessor ��� ����������� � ������ ������
//     * @param save            ���� True ������ ���������� � ������ ���� ��������� � ����
//     * @return ���������� ������������ �������
//     */
//    public int iterate(DBObject filter, IDBObjectProcessor objectProcessor, boolean save) {
//        return iterate(filter, objectProcessor, save, null);
//    }
//
//    /**
//     * ������� �������� � ���� ������ �������� ��� ���� ��������� ��������
//     *
//     * @param filter          ������ �� ������� ������
//     * @param objectProcessor ��� ����������� � ������ ������
//     * @param save            ���� True ������ ���������� � ������ ���� ��������� � ����
//     * @param orderBy         ������� ���������� �������
//     * @return ���������� ������������ �������
//     */
//    public int iterate(DBObject filter, IDBObjectProcessor objectProcessor, boolean save, DBObject orderBy) {
//        DBCursor cursor = collection.find(filter);
//        if (orderBy != null)
//            cursor.sort(orderBy);
//        while (cursor.hasNext()) {
//            DBObject obj = cursor.next();
//            objectProcessor.process(obj);
//            if (save)
//                collection.save(obj);
//        }
//        LOGGER.info("Iterate - " + filter.toString() + ", collection full name - " + collection.getFullName() + " count - " + cursor.count());
//        return cursor.count();
//    }
//
//    private ArrayList<DBObject> toList(DBCursor cursor) {
//        ArrayList<DBObject> result = new ArrayList<DBObject>();
//        while (cursor.hasNext()) {
//            DBObject obj = cursor.next();
//            result.add(obj);
//        }
//        return result;
//    }
//
//    public DBObject findById(String id) {
//        return findById(new ObjectId(id));
//    }
//
//    public DBObject findById(ObjectId id) {
//        DBObject obj = new BasicDBObject();
//        obj.put(ID, id);
//        return findOne(obj);
//    }
//
//    /**
//     * �������� ������ �������� �� ������� ���������, ��������������� �������
//     *
//     * @param obj - ������ ������
//     * @return ������ ��������
//     */
//    public List<DBObject> find(DBObject obj) {
//        Date start = DateUtils.now();
//        List<DBObject> result = toList(collection.find(obj));
//        logSlowQuery(start, "find(DBObject obj='%s')", obj.toString());
//        LOGGER.info("Search - " + obj.toString() + ", collection full name - " + collection.getFullName() + " count - " + result.size());
//        return result;
//    }
//
//    private void logSlowQuery(Date start, String methodFmt, Object... args) {
//        Date now = DateUtils.now();
//        long len = now.getTime() - start.getTime();
//        if (len > SLOW_TIME)
//            SLOW_LOG.debug(String.format("Time: %d, Collection: %s, Call: %s", len, collection.getName(), String.format(methodFmt, args)));
//    }
//
//    /**
//     * �������� ������ �������� �� ������� ���������, ��������������� �������, �� �� ������ limit
//     *
//     * @param obj   - ������ ������
//     * @param limit - ������ ��� ������
//     * @return ������ ��������
//     */
//    public List<DBObject> find(DBObject obj, int limit) {
//        Date start = DateUtils.now();
//        List<DBObject> result = toList(collection.find(obj).limit(limit));
//        logSlowQuery(start, "find(DBObject obj='%s', int limit=%d)", obj.toString(), limit);
//        LOGGER.info("Search - " + obj.toString() + ", collection full name - " + collection.getFullName() + " count - " + result.size() + "limit - " + limit);
//        return result;
//    }
//
//    /**
//     * �������� ������ �������� �� ������� ���������, ��������������� �������, ��������������� � ������������ orderBy, �� �� ������ limit
//     *
//     * @param obj     ������ ������
//     * @param skip    ���������� ������������ �������
//     * @param limit   ������ ��� ������
//     * @param orderBy ��������� ����������
//     * @return ������ ��������
//     */
//    public List<DBObject> find(DBObject obj, int skip, int limit, DBObject orderBy) {
//        Date start = DateUtils.now();
//        DBCursor dbCursor = collection.find(obj).sort(orderBy);
//        if (skip > 0)
//            dbCursor = dbCursor.skip(skip);
//        if (limit > 0)
//            dbCursor = dbCursor.limit(limit);
//        List<DBObject> result = toList(dbCursor);
//        logSlowQuery(start, "find(DBObject obj='%s', int limit=%d, DBObject orderBy='%s')", obj.toString(), limit, orderBy.toString());
//        LOGGER.info("Search - " + obj.toString() + ", collection full name - " + collection.getFullName() + " count - " + result.size() + "skip - " + skip + "limit - " + limit);
//        return result;
//    }
//
//    public int count(DBObject obj) {
//        return count(obj, 0, 0);
//    }
//
//    public int count(DBObject obj, int skip, int limit) {
//        Date start = DateUtils.now();
//        DBCursor cur = collection.find(obj);
//        if (skip > 0)
//            cur.skip(skip);
//        if (limit > 0)
//            cur.limit(limit);
//        int result = cur.count();
//        logSlowQuery(start, "count(DBObject obj='%s')", obj.toString());
//        return result;
//    }
//
//    public DBObject findOne(DBObject obj) {
//        Date start = DateUtils.now();
//        DBObject result = null;
//        List<DBObject> items = toList(collection.find(obj));
//        logSlowQuery(start, "findOne(DBObject obj='%s')", obj.toString());
//        if (items.size() == 1) {
//            result = items.get(0);
//        } else if (items.size() > 1) {
//            throw new IllegalStateException("More then one object found");
//        }
//
//        return result;
//    }
//
//    public static BasicDBObject buildDBObject(Map<String, Object> properties) {
//        BasicDBObject obj = new BasicDBObject();
//        for (Map.Entry<String, Object> entry : properties.entrySet()) {
//            obj = obj.append(entry.getKey(), entry.getValue());
//        }
//        return obj;
//    }
//
//    public void dropRepository() {
//        collection.drop();
//    }
//
//    private DBCollection resolveCollection(Config config, String collectionName) throws UnknownHostException {
//        if (server == null) {
//            server = createServer(config.getDatabaseHost(), config.getDatabasePort());
//            database = server.getDB(config.getDatabaseName());
//        }
//        return database.getCollection(collectionName);
//    }
//
//    private static Mongo createServer(String host, int port) throws UnknownHostException {
//        MongoOptions options = new MongoOptions();
//        options.autoConnectRetry = true;
//        options.connectionsPerHost = 100;
//        options.connectTimeout = 30000;
//        options.socketTimeout = 60000;
//        options.threadsAllowedToBlockForConnectionMultiplier = 1500;
//        return new Mongo(new ServerAddress(host, port), options);
//    }
//
//    public static void dropDatabase(Config config) throws UnknownHostException {
//        Mongo server = createServer(config.getDatabaseHost(), config.getDatabasePort());
//        DB database = server.getDB(config.getDatabaseName());
//        database.dropDatabase();
//        LOGGER.info("Database {} dropped", config.getDatabaseName());
//    }
//
//    public void ensureIndex(DBObject keys) {
//        collection.ensureIndex(keys);
//    }
//
//    public void update(DBObject query, DBObject value) {
//        Date start = DateUtils.now();
//        collection.update(query, value, false, true);
//        logSlowQuery(start, "update(DBObject query='%s', DBObject value='%s')", query.toString(), value.toString());
//    }
//
//    public List distinct(String field, DBObject query) {
//        Date start = DateUtils.now();
//        List result = collection.distinct(field, query);
//        logSlowQuery(start, "distinct(String field='%s', DBObject query='%s')", field, query.toString());
//        return result;
//    }
//
//    public List<String> distinctStrings(String field, DBObject query) {
//        Date start = DateUtils.now();
//        List res = collection.distinct(field, query);
//        logSlowQuery(start, "distinctStrings(String field='%s', DBObject query='%s')", field, query.toString());
//        List<String> result = new ArrayList<String>();
//        for (Object obj : res)
//            result.add(obj.toString());
//        Collections.sort(result);
//        return result;
//    }
//
//    public void save(DBObject object) {
//        collection.save(object);
//    }
}
