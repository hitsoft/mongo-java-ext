package com.hitsoft.mongo.managed;

import com.hitsoft.mongo.basic.DBObjectExt;
import com.hitsoft.mongo.basic.SearchBuilder;

/**
 * Created by IntelliJ IDEA.
 * User: smeagol
 * Date: 08.11.11
 * Time: 13:16
 */
public class ManagedSearchBuilder extends SearchBuilder {
    public static SearchBuilder byId(ManagedObject obj) {
        return start().equal(ManagedObject.Field._ID, obj.id());
    }

    public static SearchBuilder bySample(ManagedObject obj) throws IllegalAccessException {
        DBObjectExt res = ManagedRepository.saveToDBObjectExt(obj);
        res.setId(null);
        return start().equal(res);
    }
}
