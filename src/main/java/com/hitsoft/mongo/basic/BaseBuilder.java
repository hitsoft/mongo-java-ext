package com.hitsoft.mongo.basic;

/**
 * User: smeagol
 * Date: 03.11.11
 * Time: 22:08
 */
public class BaseBuilder {

    protected DBObjectBuilder obj = DBObjectBuilder.start();

    public DBObjectExt get() {
        return obj.get();
    }
}
