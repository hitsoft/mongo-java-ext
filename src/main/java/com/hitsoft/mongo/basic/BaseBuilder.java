package com.hitsoft.mongo.basic;

import com.mongodb.DBObject;

/**
 * User: smeagol
 * Date: 03.11.11
 * Time: 22:08
 */
public class BaseBuilder {

    protected DBObjectBuilder obj = DBObjectBuilder.start();

    public DBObject get() {
        return obj.get().asDBObject();
    }

    public DBObjectExt getExt() {
        return obj.get();
    }
}
