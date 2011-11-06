package com.hitsoft.mongo.basic;

import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: smeagol
 * Date: 02.11.11
 * Time: 18:14
 */
public class Connection {
    private DB database = null;

    private static Connection instance;

    public static Connection getInstance() {
        assert instance != null : "Singletone Connection should be configured with function Connection.setupInstance() first.";
        return instance;
    }

    private void createServer(String host, int port, String dbName) throws UnknownHostException {
        MongoOptions options = new MongoOptions();
        options.autoConnectRetry = true;
        options.connectionsPerHost = 100;
        options.connectTimeout = 30000;
        options.socketTimeout = 60000;
        options.threadsAllowedToBlockForConnectionMultiplier = 1500;
        Mongo server = new Mongo(new ServerAddress(host, port), options);
        database = server.getDB(dbName);
    }

    public static void setupInstance(String host, int port, String dbName) throws UnknownHostException {
        instance = new Connection();
        instance.createServer(host, port, dbName);
    }

    public static void setupTestInstance() throws UnknownHostException {
        setupInstance("localhost", 27017, "mongo-java-ext-test");
    }

    public DBCollection getCollection(String name) {
        return database.getCollection(name);
    }

    public void dropDatabase() {
        database.dropDatabase();
    }

    public Set<String> getCollectionNames() {
        return database.getCollectionNames();
    }

}
